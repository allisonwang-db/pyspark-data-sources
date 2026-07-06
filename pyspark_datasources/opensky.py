"""
OpenSky Network Data Source for Apache Spark - Academic/Private Use Example

This module provides a custom Spark data source for streaming real-time aircraft
tracking data from the OpenSky Network API (https://opensky-network.org/).

Fixes over the original implementation:

1. CRASH WITH client_id/client_secret (the reported bug):
   The original reader called `_get_access_token()` (a network request) inside
   `__init__`. Spark instantiates the reader during query planning inside a
   *forked* `pyspark.daemon` worker. On macOS, `requests` consults the system
   proxy configuration via the SystemConfiguration framework
   (`urllib.request.proxy_bypass_macosx_sysconf`), which is not fork-safe and
   segfaults the worker ("Python worker exited unexpectedly (crashed)").
   Fix: no network I/O in `__init__` — the OAuth token is fetched lazily on
   first read, and the HTTP session is created with `trust_env=False` so no
   fork-unsafe system proxy lookup ever happens (explicit HTTPS_PROXY/HTTP_PROXY
   environment variables are still honored manually).

2. PICKLE / FORK SAFETY:
   The reader is pickled between the driver and worker processes. The
   `requests.Session` (which may hold live SSL sockets) is now excluded from
   pickling and recreated lazily in each process.

3. TOKEN REFRESH USES THE RETRYING SESSION:
   The token request now goes through the same retry-enabled session as data
   requests, and the token is transparently refreshed before expiry.

4. CORRECT FIELD MAPPING:
   Per the OpenSky API documentation, state[7] is *barometric* altitude,
   state[13] is *geometric* altitude, state[16] is position_source, and the
   aircraft `category` is only returned as state[17] when the request is made
   with `extended=1`. The original mapped state[7]→geo_altitude,
   state[13]→baro_altitude and state[16]→category. The schema is unchanged, but
   the values are now correct and `extended=1` is requested.

5. SAFE TIMESTAMP PARSING:
   `time_position` (state[3]) may be null; the original crashed the whole
   micro-batch on `datetime.fromtimestamp(None)`. Timestamps are now parsed
   defensively.

6. BATCH SUPPORT:
   In addition to `spark.readStream`, `spark.read.format("opensky")` now works
   and returns one snapshot of the current state vectors.

Usage (unchanged from the original):

    df = spark.readStream.format("opensky").load()

    df = spark.readStream.format("opensky") \\
        .option("region", "EUROPE") \\
        .option("client_id", "your_research_client_id") \\
        .option("client_secret", "your_research_client_secret") \\
        .load()

Rate Limits & Responsible Usage:
    - Anonymous access: 100 API calls per day
    - Authenticated access: 4000 API calls per day (research accounts)
    - Minimum 5-second interval between requests

Data Attribution:
    When using this data in research or publications, please cite:
    "The OpenSky Network, https://opensky-network.org"

Author: Frank Munz, Databricks - Example Only, No Warranty
Purpose: Educational Example / Academic Research Tool
Version: 1.1 (fixed)

Must comply with OpenSky Network Terms of Use:
https://opensky-network.org/about/terms-of-use
"""

import logging
import os
import time
from datetime import datetime, timezone
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, Iterator, List, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from pyspark.sql.datasource import (
    DataSource,
    DataSourceReader,
    InputPartition,
    SimpleDataSourceStreamReader,
)
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

DS_NAME = "opensky"

TOKEN_URL = (
    "https://auth.opensky-network.org/auth/realms/opensky-network"
    "/protocol/openid-connect/token"
)
STATES_URL = "https://opensky-network.org/api/states/all"


@dataclass
class BoundingBox:
    lamin: float  # Minimum latitude
    lamax: float  # Maximum latitude
    lomin: float  # Minimum longitude
    lomax: float  # Maximum longitude


class Region(Enum):
    EUROPE = BoundingBox(35.0, 72.0, -25.0, 45.0)
    NORTH_AMERICA = BoundingBox(7.0, 72.0, -168.0, -60.0)
    SOUTH_AMERICA = BoundingBox(-56.0, 15.0, -90.0, -30.0)
    ASIA = BoundingBox(-10.0, 82.0, 45.0, 180.0)
    AUSTRALIA = BoundingBox(-50.0, -10.0, 110.0, 180.0)
    AFRICA = BoundingBox(-35.0, 37.0, -20.0, 52.0)
    GLOBAL = BoundingBox(-90.0, 90.0, -180.0, 180.0)


logger = logging.getLogger(__name__)


class OpenSkyAPIError(Exception):
    """Base exception for OpenSky API errors"""


class TransientOpenSkyError(OpenSkyAPIError):
    """A fault expected to clear on its own: network timeouts, connection
    errors (including egress blocks), and rate limiting. The streaming reader
    swallows these and skips the micro-batch so the stream auto-recovers when
    the upstream becomes reachable again."""


class ConfigOpenSkyError(OpenSkyAPIError):
    """A deterministic fault that will NOT self-heal: bad/missing credentials,
    a malformed token response, or any auth failure. The streaming reader lets
    these propagate so the pipeline update fails loudly with an event-log entry
    instead of silently emitting zero rows."""


class RateLimitError(TransientOpenSkyError):
    """Raised when API rate limit is exceeded (HTTP 429). Transient: back off,
    do not kill the stream."""


class OpenSkyClient:
    """Fork- and pickle-safe OpenSky API client with lazy OAuth2 authentication.

    No network I/O happens at construction time; the HTTP session and the
    access token are created on first use in whichever process performs the
    read. The live session object is never pickled.
    """

    MIN_REQUEST_INTERVAL = 5.0  # seconds between requests
    MAX_RETRIES = 3
    RETRY_BACKOFF = 2
    RETRY_STATUS_CODES = [429, 500, 502, 503, 504]
    TOKEN_REFRESH_MARGIN = 60  # refresh this many seconds before expiry

    def __init__(self, bbox: BoundingBox, client_id: Optional[str], client_secret: Optional[str]):
        self.bbox = bbox
        self.client_id = client_id
        self.client_secret = client_secret
        self._session: Optional[requests.Session] = None
        self._access_token: Optional[str] = None
        self._token_expires_at: float = 0.0
        self._last_request_time: float = 0.0

    # -- pickling: never carry a live session (SSL sockets) across processes
    def __getstate__(self) -> Dict[str, Any]:
        state = self.__dict__.copy()
        state["_session"] = None
        return state

    @property
    def session(self) -> requests.Session:
        if self._session is None:
            session = requests.Session()
            # trust_env=False prevents requests/urllib from querying the OS
            # proxy settings (fork-unsafe on macOS, segfaults forked Spark
            # workers). Explicit proxy environment variables still work.
            session.trust_env = False
            proxies = {
                scheme: os.environ[var]
                for scheme, var in (("http", "HTTP_PROXY"), ("https", "HTTPS_PROXY"))
                if os.environ.get(var)
            }
            if proxies:
                session.proxies.update(proxies)
            retry_strategy = Retry(
                total=self.MAX_RETRIES,
                backoff_factor=self.RETRY_BACKOFF,
                status_forcelist=self.RETRY_STATUS_CODES,
            )
            adapter = HTTPAdapter(max_retries=retry_strategy)
            session.mount("https://", adapter)
            session.mount("http://", adapter)
            self._session = session
        return self._session

    def _ensure_token(self) -> None:
        """Fetch or refresh the OAuth2 token (client credentials flow)."""
        if not (self.client_id and self.client_secret):
            return
        now = time.time()
        if self._access_token and now < self._token_expires_at:
            return

        data = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }
        try:
            response = self.session.post(TOKEN_URL, data=data, timeout=10)
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            # Reaching the token endpoint failed transiently (egress/network).
            raise TransientOpenSkyError(f"Token request failed transiently: {e}") from e
        except requests.exceptions.RequestException as e:
            # Non-transient HTTP error (e.g. 400/401 for bad client credentials).
            raise ConfigOpenSkyError(f"Failed to get access token: {e}") from e

        # A 4xx on the token endpoint means bad/rejected credentials: deterministic.
        if response.status_code in (400, 401, 403):
            raise ConfigOpenSkyError(
                f"Authentication failed (HTTP {response.status_code}): check "
                f"client_id/client_secret"
            )
        try:
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            raise TransientOpenSkyError(f"Token endpoint error: {e}") from e

        # Parse the token body inside the guard: a malformed 200 body (e.g. an
        # OAuth error JSON with no access_token) must surface as a clear auth
        # error, not a bare KeyError that escapes the OpenSkyAPIError contract.
        try:
            token_data = response.json()
            self._access_token = token_data["access_token"]
        except (ValueError, KeyError, TypeError) as e:
            raise ConfigOpenSkyError(
                f"Malformed token response (no access_token): {e}"
            ) from e
        expires_in = token_data.get("expires_in", 1800)
        self._token_expires_at = now + expires_in - self.TOKEN_REFRESH_MARGIN

    def _throttle(self) -> None:
        """Keep at least MIN_REQUEST_INTERVAL seconds between requests."""
        elapsed = time.time() - self._last_request_time
        if elapsed < self.MIN_REQUEST_INTERVAL:
            time.sleep(self.MIN_REQUEST_INTERVAL - elapsed)
        self._last_request_time = time.time()

    def fetch_states(self) -> Dict[str, Any]:
        """Fetch current state vectors, authenticating if credentials are set."""
        self._throttle()
        self._ensure_token()

        params = {
            "lamin": self.bbox.lamin,
            "lamax": self.bbox.lamax,
            "lomin": self.bbox.lomin,
            "lomax": self.bbox.lomax,
            "extended": 1,  # include aircraft category as state[17]
        }
        headers = {}
        if self._access_token:
            headers["Authorization"] = f"Bearer {self._access_token}"

        try:
            response = self.session.get(STATES_URL, params=params, headers=headers, timeout=10)
            if response.status_code == 429:
                raise RateLimitError("API rate limit exceeded")
            if response.status_code == 401 and self._access_token:
                # Token revoked or expired early: refresh once and retry.
                self._access_token = None
                self._ensure_token()
                headers["Authorization"] = f"Bearer {self._access_token}"
                response = self.session.get(
                    STATES_URL, params=params, headers=headers, timeout=10
                )
            if response.status_code == 401:
                # Still unauthorized after a token refresh: credentials are bad,
                # not a transient blip. Fail loudly.
                raise ConfigOpenSkyError(
                    "Authentication failed (HTTP 401) after token refresh: "
                    "check client_id/client_secret"
                )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.Timeout as e:
            raise TransientOpenSkyError("API request timed out") from e
        except requests.exceptions.ConnectionError as e:
            # Includes egress blocks (dropped SYN → ConnectTimeout): self-heals.
            raise TransientOpenSkyError("Connection error occurred") from e
        except requests.exceptions.RequestException as e:
            # Retries on 5xx/429 are exhausted here (RetryError) or a 4xx slipped
            # through: treat as transient so a flaky endpoint doesn't kill the
            # stream. Deterministic auth (401) is already raised above.
            raise TransientOpenSkyError(f"API request failed: {e}") from e


def _safe_float(value: Any) -> Optional[float]:
    try:
        return float(value) if value is not None else None
    except (ValueError, TypeError):
        return None


def _safe_int(value: Any) -> Optional[int]:
    try:
        return int(value) if value is not None else None
    except (ValueError, TypeError):
        return None


def _safe_bool(value: Any) -> Optional[bool]:
    return bool(value) if value is not None else None


def _safe_ts(value: Any) -> Optional[datetime]:
    try:
        return datetime.fromtimestamp(value, tz=timezone.utc) if value is not None else None
    except (ValueError, TypeError, OSError):
        return None


def valid_state(state: List) -> bool:
    """A usable state vector has at least the 17 standard fields and a position."""
    if not state or len(state) < 17:
        return False
    return (
        state[0] is not None  # icao24
        and state[5] is not None  # longitude
        and state[6] is not None  # latitude
    )


def parse_state(state: List, timestamp: int) -> Tuple:
    """Parse one state vector (extended=1 layout) with safe type conversion."""
    return (
        _safe_ts(timestamp),  # time_ingest
        state[0],  # icao24
        state[1],  # callsign
        state[2],  # origin_country
        _safe_ts(state[3]),  # time_position (may be null)
        _safe_ts(state[4]),  # last_contact
        _safe_float(state[5]),  # longitude
        _safe_float(state[6]),  # latitude
        _safe_float(state[13]) if len(state) > 13 else None,  # geo_altitude
        _safe_bool(state[8]),  # on_ground
        _safe_float(state[9]),  # velocity
        _safe_float(state[10]),  # true_track
        _safe_float(state[11]),  # vertical_rate
        state[12],  # sensors
        _safe_float(state[7]),  # baro_altitude
        state[14],  # squawk
        _safe_bool(state[15]),  # spi
        _safe_int(state[17]) if len(state) > 17 else None,  # category (extended=1)
    )


def parse_response(data: Dict[str, Any]) -> List[Tuple]:
    ts = data.get("time", int(time.time()))
    return [parse_state(s, ts) for s in (data.get("states") or []) if valid_state(s)]


def _make_client(options: Dict[str, str]) -> OpenSkyClient:
    region_name = options.get("region", "NORTH_AMERICA").upper()
    try:
        bbox = Region[region_name].value
    except KeyError:
        logger.warning("Invalid region '%s'. Defaulting to NORTH_AMERICA.", region_name)
        bbox = Region["NORTH_AMERICA"].value
    return OpenSkyClient(bbox, options.get("client_id"), options.get("client_secret"))


class OpenSkyStreamReader(SimpleDataSourceStreamReader):
    def __init__(self, schema: StructType, options: Dict[str, str]):
        super().__init__()
        self.schema = schema
        self.client = _make_client(options)

    def initialOffset(self) -> Dict[str, int]:
        return {"last_fetch": 0}

    def read(self, start: Dict[str, int]) -> Tuple[List[Tuple], Dict[str, int]]:
        """Read the current state vectors and advance the offset.

        Error policy (see the exception taxonomy in this module):
          * TransientOpenSkyError (timeouts, connection/egress errors, 429) is
            swallowed: we log a warning and return the offset unchanged so no
            micro-batch is triggered, and the stream auto-recovers once the
            upstream is reachable again.
          * Everything else — ConfigOpenSkyError (bad creds, malformed token),
            and any unexpected programming error (parse bugs, etc.) — is allowed
            to propagate. That fails the Structured Streaming micro-batch (and
            surfaces as a StreamingQueryException / a failed pipeline update)
            instead of silently emitting zero rows.

        Note this reader is a driver-side SimpleDataSourceStreamReader, so
        `logger` output lands in the driver log. It is NOT an event-log event;
        only a raised exception surfaces there. That is deliberate: a transient
        blip should stay quiet and self-heal, a deterministic fault should be loud.
        """
        try:
            data = self.client.fetch_states()
            return (parse_response(data), {"last_fetch": data.get("time", int(time.time()))})
        except TransientOpenSkyError as e:
            # Do not advance the offset: Spark sees "no new data" and skips the
            # batch. Re-fetch happens on the next trigger (the snapshot is always
            # current, so nothing is lost by not advancing).
            logger.warning("OpenSky transient error, skipping micro-batch: %s", e)
            return ([], start)

    def readBetweenOffsets(self, start: Dict[str, int], end: Dict[str, int]) -> Iterator[Tuple]:
        # The API cannot replay past data; re-fetch the latest snapshot.
        data, _ = self.read(start)
        return iter(data)


class OpenSkyBatchReader(DataSourceReader):
    """Batch reader: one snapshot of the current state vectors per read."""

    def __init__(self, schema: StructType, options: Dict[str, str]):
        self.schema = schema
        self.options = dict(options)

    def partitions(self) -> List[InputPartition]:
        return [InputPartition(0)]

    def read(self, partition: InputPartition) -> Iterator[Tuple]:
        client = _make_client(self.options)
        return iter(parse_response(client.fetch_states()))


class OpenSkyDataSource(DataSource):
    """
    Apache Spark DataSource for real-time aircraft tracking data from the
    OpenSky Network (https://opensky-network.org/).

    Options
    -------
    region : str, default "NORTH_AMERICA"
        One of EUROPE, NORTH_AMERICA, SOUTH_AMERICA, ASIA, AUSTRALIA, AFRICA,
        GLOBAL.
    client_id / client_secret : str, optional
        OAuth2 client credentials for authenticated access (higher rate
        limits). Both must be provided together.

    Examples
    --------
    >>> spark.dataSource.register(OpenSkyDataSource)

    Streaming, anonymous:

    >>> df = spark.readStream.format("opensky").load()

    Streaming, authenticated:

    >>> df = spark.readStream.format("opensky") \\
    ...     .option("region", "EUROPE") \\
    ...     .option("client_id", "your_client_id") \\
    ...     .option("client_secret", "your_client_secret") \\
    ...     .load()

    Batch snapshot:

    >>> df = spark.read.format("opensky").option("region", "EUROPE").load()

    When using this data in research, please cite "The OpenSky Network,
    https://opensky-network.org" and the OpenSky IPSN 2014 paper.
    """

    def __init__(self, options: Dict[str, str] = None):
        super().__init__(options or {})
        self.options = options or {}

        if self.options.get("client_id") and not self.options.get("client_secret"):
            raise ValueError("client_secret must be provided when client_id is set")
        if self.options.get("client_secret") and not self.options.get("client_id"):
            raise ValueError("client_id must be provided when client_secret is set")

        if "region" in self.options and self.options["region"].upper() not in Region.__members__:
            raise ValueError(
                f"Invalid region. Must be one of: {', '.join(Region.__members__.keys())}"
            )

    @classmethod
    def name(cls) -> str:
        return DS_NAME

    def schema(self) -> StructType:
        return StructType(
            [
                StructField("time_ingest", TimestampType()),
                StructField("icao24", StringType()),
                StructField("callsign", StringType()),
                StructField("origin_country", StringType()),
                StructField("time_position", TimestampType()),
                StructField("last_contact", TimestampType()),
                StructField("longitude", DoubleType()),
                StructField("latitude", DoubleType()),
                StructField("geo_altitude", DoubleType()),
                StructField("on_ground", BooleanType()),
                StructField("velocity", DoubleType()),
                StructField("true_track", DoubleType()),
                StructField("vertical_rate", DoubleType()),
                StructField("sensors", ArrayType(IntegerType())),
                StructField("baro_altitude", DoubleType()),
                StructField("squawk", StringType()),
                StructField("spi", BooleanType()),
                StructField("category", IntegerType()),
            ]
        )

    def simpleStreamReader(self, schema: StructType) -> OpenSkyStreamReader:
        return OpenSkyStreamReader(schema, self.options)

    def reader(self, schema: StructType) -> OpenSkyBatchReader:
        return OpenSkyBatchReader(schema, self.options)
