import contextlib
import json
from ast import literal_eval
from typing import Any, Generator, Hashable
from urllib.parse import parse_qs, urlparse, urlunparse

import httpx
import pyspark.sql.types as T
import validators
from authlib.integrations.httpx_client import OAuth2Client
from authlib.oauth2.rfc7523 import ClientSecretJWT
from pyspark.sql import Row
from pyspark.sql.datasource import DataSource, DataSourceReader


class RestapiDataSource(DataSource):
    def __init__(self, options):
        super().__init__(options)
        if "path" not in options or not options["path"]:
            raise Exception("You must specify the URL to read from in `.load()`.")

    @classmethod
    def name(cls):
        return "restapi"

    def schema(self):
        return "struct<url: string, headers: map<string, string>, auth: map<string, string>, params: map<string, string>, data: string, json_data: map<string, string>, method: string, options: map<string, string>, result: string>"

    def reader(self, schema: T.StructType) -> DataSourceReader:
        return RestapiRequestReader(self.options)

class RestapiRequestReader(DataSourceReader):
    def __init__(self, options: dict):
        self.protocol = str(options.get("protocol", "https")).strip()
        _ = self.valid_method(self.protocol)

        _ = validators.url(options.get("path"))
        self.url = f"{self.protocol}://{options.get('path')}"

        self.headers = self._get_dict_option("headers", options) or {}

        self.auth = self._get_dict_option("auth", options) or { "type": "NO_AUTH" }
        self.auth["type"] = str(self.auth.get("type")).strip().upper()
        _ = self.valid_auth(self.auth)

        self.params = self._get_dict_option("params", options) or {}

        self.data = str(options.get("data", "")).strip()
        _ = self.valid_data(self.data)

        self.json_data = self._get_dict_option("json_data", options) or {}
        _ = self.valid_json_data(self.json_data)

        self.method = str(options.get("method", "GET")).strip().upper()
        _ = self.valid_method(self.method)

        # For options, see: https://www.python-httpx.org/api
        # Advanced use, they have the potential to overwrite the other exposed options
        self.options = self._get_dict_option("options", options) or {}

    def read(self, partition):
        for row in self._batch_request():
            yield Row(
                url=self.url,
                headers=self.headers,
                auth=self.auth,
                params=self.params,
                data=self.data,
                json_data=self.json_data,
                method=self.method,
                options=self.options,
                result=row
            )

    def _get_dict_option(self, key: str, options: dict) -> dict:
        """Get a dictionary option from the options. Spark automatically transforms them to string.

        Parameters
        ----------
        key : str
            Option key, as passed to `spark.read.option(key, value)`
        options : dict
            Options dictionary

        Raises
        ------
            TypeError
                Option value is expected to be of either type str or dict

        Returns
        -------
        dict
            Dictionary containing requested option
        """
        option_value = options.get(key)

        if isinstance(option_value, str):
            return literal_eval(option_value)

        if not isinstance(option_value, dict):
            msg = f"Expected {key} to be either str or dict. Got {type(option_value)}"
            raise TypeError(msg)

        return option_value

    @validators.validator
    def valid_protocol(value):
        return isinstance(value, str) and (value in ["http", "https"])

    @validators.validator
    def valid_auth(value) -> bool:
        return value and isinstance(value, dict) and (str(value.get("type")).strip().upper() in ["NO_AUTH", "BASIC", "OAUTH2"])

    @validators.validator
    def valid_basic_auth(value) -> bool:
        return value and value.get("client-id") and value.get("client-secret")

    @validators.validator
    def valid_oauth(value) -> bool:
        return value and value.get("token-url") and value.get("client-id") and value.get("client-secret")

    @validators.validator
    def valid_data(value) -> bool:
        return isinstance(value, str)

    @validators.validator
    def valid_json_data(value) -> bool:
        return isinstance(value, dict)

    @validators.validator
    def valid_method(value) -> bool:
        return isinstance(value, str) and (value in ["GET", "POST"])

    def _build_no_auth_client(self) -> httpx.Client:
        """Create a client with no authentication.

        Returns
        -------
        httpx.Client
            Client object to run the requests from.

        """
        transport = httpx.HTTPTransport(retries=5)
        return httpx.Client(transport=transport)

    def _build_basic_auth_client(self):
        """Create a Basic Auth client using the given authentication information.

        Returns
        -------
        httpx.Client
            Client object to run the requests from.

        Raises
        ------
        ValidationFailure
            Auth should not be null, and the client-id and client-secret fields must exist.

        """

        _ = self.valid_basic_auth(self.auth)

        client_id = self.auth.get("client-id")
        client_secret = self.auth.get("client-secret")

        basic_auth = httpx.BasicAuth(username=client_id, password=client_secret)
        transport = httpx.HTTPTransport(retries=5)

        return httpx.Client(transport=transport, auth=basic_auth)

    def _build_oauth_client(self):
        """Create an OAuth2 client using the given authentication information.

        Returns
        -------
        httpx.Client
            Client object to run the requests from.

        Raises
        ------
        ValidationFailure
            Auth should not be null, and the fields token-url, client-id and
            client-secret must exist.

        """
        _ = self.valid_oauth(self.auth)

        token_url = self.auth.get("token-url")
        client_id = self.auth.get("client-id")
        client_secret = self.auth.get("client-secret")

        client = OAuth2Client(
            client_id=client_id,
            client_secret=client_secret,
            token_endpoint_auth_method="client_secret_jwt",
        )
        client.register_client_auth_method(ClientSecretJWT(token_endpoint=token_url))
        client.fetch_token(token_url)

        return client

    def _build_client(self) -> httpx.Client:
        """Create a client with the given authentication information.

        Returns
        -------
        httpx.Client
            Client object to run the requests from.

        Raises
        ------
        ValidationFailure
            Auth should not be null, and the field type within should have one of the
            values: NO_AUTH, BASIC, OAUTH2

        """
        auth_type = self.auth.get("type")

        if auth_type == "NO_AUTH":
            return self._build_no_auth_client()
        if auth_type == "BASIC":
            return self._build_basic_auth_client()
        if auth_type == "OAUTH2":
            return self._build_oauth_client()

    def _split_params_from_url(self) -> tuple[str, dict]:
        """Split the URL into the base URL and the query parameters.

        Returns
        -------
        tuple[str, dict]
            Tuple containing the URL string, stripped of parameters on position 0, and a
            parameter dictionary derived from the URL on position 1.

        """
        parsed_url = urlparse(self.url)
        derived_params = parse_qs(parsed_url.query)
        stripped_url = urlunparse(
            (
                parsed_url.scheme,
                parsed_url.netloc,
                parsed_url.path,
                "",
                "",
                parsed_url.fragment,
            ),
        )
        return stripped_url, derived_params

    def _update_url_and_params(self) -> tuple[str, dict]:
        """Update the URL and its parameters with the given parameters.

        Returns
        -------
        tuple[str, dict]
            Tuple containing the URL string, stripped of parameters, and a dictionary
            containing a merge of the parameters derived from the URL and the explicit
            parameters, where the explicit parameters override any derived ones.

        """
        stripped_url, derived_params = self._split_params_from_url()

        # Explicitly set parameters take precedence over the ones derived from the URL
        derived_params.update(self.params)

        return stripped_url, derived_params

    def _add_data_for_get_request(self) -> dict:
        """Prepare the data field sent in the body of a GET request.

        Returns
        -------
        dict
            Dictionary containing the data prepared to be sent as a
            keyword argument.

        Raises
        ------
        TypeError
            Provided data can only be a string or dict.

        """
        kwargs: dict[Hashable, Any] = {}

        if (not self.data) and (not self.json_data):
            return kwargs

        if isinstance(self.json_data, dict) and self.json_data:
            kwargs["data"] = json.dumps(self.json_data)
            return kwargs

        if isinstance(self.data, str) and self.data:
            kwargs["data"] = self.data
            return kwargs

        type_error_msg = f"Expected either data to be str or json_data to be dict. Got data: {type(self.data)} and json_data: {type(self.json_data)}"
        raise TypeError(type_error_msg)

    def _add_data_for_post_request(self) -> dict:
        """Prepare the data field sent in the body of a POST request.

        Returns
        -------
        dict
            Dictionary containing the data prepared to be sent as a
            keyword argument.

        Raises
        ------
        TypeError
            Provided data can only be a string or dict.

        """
        kwargs: dict[Hashable, Any] = {}

        if (not self.data) and (not self.json_data):
            return kwargs

        if isinstance(self.json_data, dict) and self.json_data:
            kwargs["json"] = self.json_data
            return kwargs

        if isinstance(self.data, str) and self.data:
            kwargs["data"] = self.data
            return kwargs

        type_error_msg = f"Expected either data to be str or json_data to be dict. Got data: {type(self.data)} and json_data: {type(self.json_data)}"
        raise TypeError(type_error_msg)

    def _prepare_request(self, url: str | None = None, params: dict | None = None) -> dict:
        """Prepare the GET or POST request with the given parameters.

        Parameters
        ----------
        url : str, optional
            URL of the endpoint, by default None
        params : dict, optional
            Parameters to be sent as part of the request, by default None


        Returns
        -------
        dict
            Dictionary of keyword arguments to be passed to the httpx.request function.

        """
        kwargs = { "url": url or self.url, "headers": self.headers, "params": params or self.params, "method": self.method }
        kwargs.update(self.options)

        if self.method.strip().upper() == "GET":
            kwargs.update(self._add_data_for_get_request())

        if self.method.strip().upper() == "POST":
            kwargs.update(self._add_data_for_post_request())

        return kwargs

    def _parse_api_response(self, res: httpx.Response) -> Generator[dict, None, None]:
        """Parse the response from the API and returns it as a generator of dictionaries.

        Parameters
        ----------
        res : httpx.Response
            HTTP response containing data.

        Returns
        -------
        Generator[dict]
            Data payload from the HTTP response, formatted as a generator of dictionaries.

        """
        res_json = None

        with contextlib.suppress(json.JSONDecodeError):
            res_json = res.json()

        if not res_json:
            res_json = [{ "payload": res.text }]

        if not isinstance(res_json, list):
            res_json = [res_json]

        for row in res_json:
            yield row

    def _batch_request(self) -> Generator[dict, None, None]:
        """Send a batch request and returns the parsed response.

        Returns
        -------
        Generator[dict]
            Data payload from the HTTP response, formatted as a generator of dictionaries.

        """
        stripped_url, stripped_params = self._update_url_and_params()
        client = self._build_client()

        kwargs = self._prepare_request(url=stripped_url, params=stripped_params)
        res = client.request(**kwargs)

        for row in self._parse_api_response(res):
            yield row
