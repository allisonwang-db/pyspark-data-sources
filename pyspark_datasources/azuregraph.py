"""Microsoft Graph API (Azure) data source - multi-endpoint connector."""

import json
import urllib.parse

import requests
from pyspark.sql import Row
from pyspark.sql.datasource import DataSource, DataSourceReader


class AzureGraphDataSource(DataSource):
    """
    A DataSource for reading from Microsoft Graph API (https://graph.microsoft.com/v1.0).
    Multi-endpoint: supports users, groups, organization, me, me/memberOf, etc.
    Requires OAuth2 token. Name: `azuregraph`
    Schema: `id string, displayName string, mail string, userPrincipalName string,
    description string, jobTitle string, raw string`
    """

    @classmethod
    def name(cls):
        return "azuregraph"

    def schema(self):
        return (
            "id string, displayName string, mail string, userPrincipalName string, "
            "description string, jobTitle string, raw string"
        )

    def reader(self, schema):
        return AzureGraphReader(self.options)


class AzureGraphReader(DataSourceReader):
    BASE = "https://graph.microsoft.com/v1.0"

    def __init__(self, options):
        self.options = options
        self.token = options.get("token") or options.get("access_token")
        if not self.token:
            raise ValueError(
                "Azure Graph data source requires 'token' or 'access_token' option. "
                "Obtain via: az account get-access-token --resource https://graph.microsoft.com"
            )
        self.endpoint = (options.get("path") or options.get("endpoint") or "users").strip("/")
        self.top = options.get("top")
        self.select = options.get("select")
        self.filter = options.get("filter")

    def _build_url(self, next_link=None):
        if next_link:
            return next_link
        url = f"{self.BASE}/{self.endpoint}"
        params = []
        if self.top:
            params.append(("$top", str(self.top)))
        if self.select:
            params.append(("$select", self.select))
        if self.filter:
            params.append(("$filter", self.filter))
        if params:
            url += "?" + urllib.parse.urlencode(params)
        return url

    def _to_row(self, item):
        if not isinstance(item, dict):
            return Row(
                id="",
                displayName="",
                mail="",
                userPrincipalName="",
                description="",
                jobTitle="",
                raw=json.dumps(item) if item is not None else "",
            )
        raw = json.dumps(item)
        return Row(
            id=str(item.get("id", "")),
            displayName=str(item.get("displayName", ""))[:500],
            mail=str(item.get("mail", ""))[:500],
            userPrincipalName=str(item.get("userPrincipalName", ""))[:500],
            description=str(item.get("description", ""))[:1000],
            jobTitle=str(item.get("jobTitle", ""))[:200],
            raw=raw[:10000],
        )

    def read(self, partition):
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Accept": "application/json",
        }
        url = self._build_url()
        try:
            while url:
                response = requests.get(url, headers=headers, timeout=30)
                response.raise_for_status()
                data = response.json()
                items = data.get("value", [])
                if isinstance(data, list):
                    items = data
                if not items and isinstance(data, dict) and "id" in data:
                    items = [data]
                for item in items:
                    yield self._to_row(item)
                url = data.get("@odata.nextLink") or None
        except requests.RequestException:
            return iter([])
