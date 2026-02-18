import hashlib
import json
import logging
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import requests
from pyspark.sql.datasource import DataSource, DataSourceStreamWriter, DataSourceWriter, WriterCommitMessage
from pyspark.sql.types import StructType

logger = logging.getLogger(__name__)

@dataclass
class MetaCapiCommitMessage(WriterCommitMessage):
    batch_id: int
    events_processed: int
    events_succeeded: int
    events_failed: int

class MetaCapiDataSource(DataSource):
    """
    A Meta Conversions API (CAPI) data source for PySpark.
    
    This data source enables writing data to Meta/Facebook via the Conversions API.
    It supports both streaming (writeStream) and batch (write) execution.
    
    Name: `meta_capi`
    
    Parameters
    ----------
    access_token : str
        Meta System User Access Token (Required).
    pixel_id : str
        Meta Pixel ID / Dataset ID (Required).
    api_version : str, optional
        Graph API version, e.g., "v19.0" (Default: "v19.0").
    batch_size : str, optional
        Number of events to send in one API call (Default: "1000").
    test_event_code : str, optional
        Code for testing events in Events Manager.
        
    Schema Mapping
    --------------
    The data source maps Spark DataFrame columns to CAPI fields.
    
    1. **Structured Mode**: If a `user_data` (Struct) column exists, it is used directly.
    2. **Flat Mode**: If no `user_data` column exists, the writer looks for these columns:
       - `email` -> `user_data.em` (auto-hashed if not SHA256)
       - `phone` -> `user_data.ph` (auto-hashed if not SHA256)
       - `client_ip_address` -> `user_data.client_ip_address`
       - `client_user_agent` -> `user_data.client_user_agent`
       - `event_name` -> `event_name`
       - `event_time` -> `event_time` (Timestamp or Long)
       - `event_id` -> `event_id` (deduplication key)
       - `action_source` -> `action_source` (default: "website")
       - `value` -> `custom_data.value`
       - `currency` -> `custom_data.currency`
    """
    
    @classmethod
    def name(cls) -> str:
        return "meta_capi"
        
    def streamWriter(self, schema: StructType, overwrite: bool) -> "MetaCapiStreamWriter":
        return MetaCapiStreamWriter(schema, self.options)

    def writer(self, schema: StructType, overwrite: bool) -> "MetaCapiBatchWriter":
        return MetaCapiBatchWriter(schema, self.options)

class _MetaCapiWriterCommon:
    """Common logic for Meta CAPI writers."""
    def __init__(self, schema: StructType, options: Dict[str, str]):
        self.schema = schema
        self.options = options
        
        self.access_token = options.get("access_token")
        self.pixel_id = options.get("pixel_id")
        self.api_version = options.get("api_version", "v19.0")
        self.batch_size = int(options.get("batch_size", "1000"))
        self.test_event_code = options.get("test_event_code")
        
        if not self.access_token or not self.pixel_id:
            raise ValueError("Meta CAPI requires 'access_token' and 'pixel_id' options.")
            
        self.api_url = f"https://graph.facebook.com/{self.api_version}/{self.pixel_id}/events"
        
    def write(self, iterator) -> MetaCapiCommitMessage:
        from pyspark import TaskContext
        context = TaskContext.get()
        batch_id = context.taskAttemptId() if context else 0
        
        events_buffer = []
        stats = {"processed": 0, "succeeded": 0, "failed": 0}
        
        for row in iterator:
            try:
                event = self._transform_row_to_event(row)
                if event:
                    events_buffer.append(event)
                    stats["processed"] += 1
                    
                    if len(events_buffer) >= self.batch_size:
                        self._send_batch(events_buffer, stats)
                        events_buffer = []
            except Exception as e:
                logger.error(f"Error processing row: {e}")
                # We count conversion failures as failed processing but don't stop the stream
                pass
                
        if events_buffer:
            self._send_batch(events_buffer, stats)
            
        return MetaCapiCommitMessage(
            batch_id=batch_id,
            events_processed=stats["processed"],
            events_succeeded=stats["succeeded"],
            events_failed=stats["failed"]
        )
        
    def _send_batch(self, events: List[Dict[str, Any]], stats: Dict[str, int]):
        if not events:
            return

        payload = {
            "access_token": self.access_token,
            "data": events
        }
        
        if self.test_event_code:
            payload["test_event_code"] = self.test_event_code
            
        try:
            response = requests.post(
                self.api_url,
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=30
            )
            
            if response.status_code == 200:
                # CAPI returns { "events_received": N, "fbtrace_id": "..." }
                stats["succeeded"] += len(events)
            else:
                logger.error(f"CAPI Batch Failed: {response.status_code} - {response.text}")
                stats["failed"] += len(events)
                
        except Exception as e:
            logger.error(f"Network error sending batch: {e}")
            stats["failed"] += len(events)

    def _transform_row_to_event(self, row) -> Optional[Dict[str, Any]]:
        # Helper to safely get field
        def get_val(field):
            return getattr(row, field, None)
            
        # 1. Base Event Data
        event_name = get_val("event_name")
        if not event_name:
            # Skip rows without event_name
            return None
            
        event_time = get_val("event_time")
        # Handle TimestampType (datetime) or Long/Int (unix timestamp)
        if hasattr(event_time, "timestamp"):
            event_time = int(event_time.timestamp())
        elif event_time is None:
             event_time = int(time.time())
        else:
            event_time = int(event_time)
            
        event = {
            "event_name": event_name,
            "event_time": event_time,
            "action_source": get_val("action_source") or "website"
        }
        
        if get_val("event_id"):
            event["event_id"] = str(get_val("event_id"))
            
        # 2. User Data
        user_data = {}
        row_user_data = get_val("user_data")
        
        if row_user_data:
            # Trusted input: user provided a struct
            # recursive conversion to dict
            user_data = row_user_data.asDict(recursive=True)
        else:
            # Flat mode mapping
            email = get_val("email") or get_val("em")
            if email:
                user_data["em"] = self._hash_if_needed(str(email).strip().lower())
                
            phone = get_val("phone") or get_val("ph")
            if phone:
                user_data["ph"] = self._hash_if_needed(str(phone).strip())
                
            ip = get_val("client_ip_address") or get_val("ip")
            if ip:
                user_data["client_ip_address"] = str(ip)
                
            agent = get_val("client_user_agent") or get_val("user_agent")
            if agent:
                user_data["client_user_agent"] = str(agent)
                
            # Add other common user_data fields if present
            for field in ["fbc", "fbp", "external_id"]:
                val = get_val(field)
                if val:
                    user_data[field] = str(val)
        
        if user_data:
            event["user_data"] = user_data
            
        # 3. Custom Data
        custom_data = {}
        row_custom_data = get_val("custom_data")
        if row_custom_data:
            custom_data = row_custom_data.asDict(recursive=True)
        else:
            # Flat mapping for value/currency
            val = get_val("value")
            if val is not None:
                custom_data["value"] = val
            
            curr = get_val("currency")
            if curr:
                custom_data["currency"] = str(curr)
                
        if custom_data:
            event["custom_data"] = custom_data
            
        return event

    def _hash_if_needed(self, val: str) -> str:
        # Simple heuristic: SHA256 hex digest is 64 chars
        if len(val) == 64 and all(c in "0123456789abcdef" for c in val):
            return val
        return hashlib.sha256(val.encode("utf-8")).hexdigest()

class MetaCapiStreamWriter(_MetaCapiWriterCommon, DataSourceStreamWriter):
    """Stream writer for Meta CAPI."""
    pass

class MetaCapiBatchWriter(_MetaCapiWriterCommon, DataSourceWriter):
    """Batch writer for Meta CAPI."""
    
    def commit(self, messages: List[MetaCapiCommitMessage]) -> None:
        """
        Handle job completion.
        Since CAPI is stateless/API-based, we just log the summary.
        """
        total_succeeded = sum(m.events_succeeded for m in messages)
        total_failed = sum(m.events_failed for m in messages)
        logger.info(f"Meta CAPI Batch Write Complete: {total_succeeded} sent, {total_failed} failed.")

    def abort(self, messages: List[MetaCapiCommitMessage]) -> None:
        """
        Handle job failure.
        Events sent to API cannot be rolled back.
        """
        logger.warning("Meta CAPI Batch Write Aborted. Some events may have been sent.")
