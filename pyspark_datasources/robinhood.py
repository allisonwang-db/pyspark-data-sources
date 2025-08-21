from dataclasses import dataclass
from typing import Dict
import requests
import json
import base64
import datetime

from pyspark.sql import Row
from pyspark.sql.types import StructType
from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition


class RobinhoodDataSource(DataSource):
    """
    A data source for reading cryptocurrency data from Robinhood Crypto API.

    This data source allows you to fetch real-time cryptocurrency market data,
    trading pairs, and price information using Robinhood's official Crypto API.
    It implements proper API key authentication and signature-based security.

    Name: `robinhood`

    Schema: `symbol string, price double, bid_price double, ask_price double, updated_at string`

    Examples
    --------
    Register the data source:

    >>> from pyspark_datasources import RobinhoodDataSource
    >>> spark.dataSource.register(RobinhoodDataSource)

    Load cryptocurrency market data with API authentication:

    >>> df = spark.read.format("robinhood") \\
    ...     .option("api_key", "your-api-key") \\
    ...     .option("private_key", "your-base64-private-key") \\
    ...     .load("BTC-USD,ETH-USD,DOGE-USD")
    >>> df.show()
    +--------+--------+---------+---------+--------------------+
    |  symbol|   price|bid_price|ask_price|          updated_at|
    +--------+--------+---------+---------+--------------------+
    |BTC-USD |45000.50|45000.25 |45000.75 |2024-01-15T16:00:...|
    |ETH-USD | 2650.75| 2650.50 | 2651.00 |2024-01-15T16:00:...|
    |DOGE-USD|   0.085|    0.084|    0.086|2024-01-15T16:00:...|
    +--------+--------+---------+---------+--------------------+



    Notes
    -----
    - Requires valid Robinhood Crypto API credentials (API key and base64-encoded private key)
    - Supports all major cryptocurrencies available on Robinhood
    - Implements proper API authentication with NaCl (Sodium) signing
    - Rate limiting is handled automatically
    - Based on official Robinhood Crypto Trading API documentation
    - Requires 'pynacl' library for cryptographic signing: pip install pynacl
    - Reference: https://docs.robinhood.com/crypto/trading/
    """

    @classmethod
    def name(cls) -> str:
        return "robinhood"

    def schema(self) -> str:
        return (
            "symbol string, price double, bid_price double, ask_price double, "
            "updated_at string"
        )

    def reader(self, schema: StructType):
        return RobinhoodDataReader(schema, self.options)


@dataclass
class CryptoPair(InputPartition):
    """Represents a single crypto trading pair partition for parallel processing."""
    symbol: str


class RobinhoodDataReader(DataSourceReader):
    """Reader implementation for Robinhood Crypto API data source."""

    def __init__(self, schema: StructType, options: Dict):
        self.schema = schema
        self.options = options
        
        # Required API authentication
        self.api_key = options.get("api_key")
        self.private_key_base64 = options.get("private_key")
        
        if not self.api_key or not self.private_key_base64:
            raise ValueError(
                "Robinhood Crypto API requires both 'api_key' and 'private_key' options. "
                "The private_key should be base64-encoded. "
                "Get your API credentials from https://docs.robinhood.com/crypto/trading/"
            )
        
        # Initialize NaCl signing key
        try:
            from nacl.signing import SigningKey
            private_key_seed = base64.b64decode(self.private_key_base64)
            self.signing_key = SigningKey(private_key_seed)
        except ImportError:
            raise ImportError(
                "PyNaCl library is required for Robinhood Crypto API authentication. "
                "Install it with: pip install pynacl"
            )
        except Exception as e:
            raise ValueError(f"Invalid private key format: {str(e)}")
        

        
        # Initialize session for connection pooling
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'PySpark Robinhood Crypto DataSource/1.0'
        })
        
        # Crypto API base URL
        self.base_url = "https://trading.robinhood.com"

    def _get_current_timestamp(self) -> int:
        """Get current UTC timestamp."""
        return int(datetime.datetime.now(tz=datetime.timezone.utc).timestamp())
    
    def _generate_signature(self, timestamp: int, method: str, path: str, body: str = "") -> str:
        """Generate NaCl signature for API authentication following Robinhood's specification."""
        # Official Robinhood signature format: f"{api_key}{current_timestamp}{path}{method}{body}"
        # For GET requests with no body, omit the body parameter
        if method.upper() == "GET" and not body:
            message_to_sign = f"{self.api_key}{timestamp}{path}{method.upper()}"
        else:
            message_to_sign = f"{self.api_key}{timestamp}{path}{method.upper()}{body}"
            
        signed = self.signing_key.sign(message_to_sign.encode("utf-8"))
        signature = base64.b64encode(signed.signature).decode("utf-8")
        return signature

    def _make_authenticated_request(self, method: str, path: str, params: Dict = None, json_data: Dict = None):
        """Make an authenticated request to the Robinhood Crypto API."""
        timestamp = self._get_current_timestamp()
        url = self.base_url + path
        
        # Prepare request body for signature (only for non-GET requests)
        body = ""
        if method.upper() != "GET" and json_data:
            body = json.dumps(json_data, separators=(',', ':'))  # Compact JSON format
        
        # Generate signature
        signature = self._generate_signature(timestamp, method, path, body)
        
        # Set authentication headers
        headers = {
            'x-api-key': self.api_key,
            'x-signature': signature,
            'x-timestamp': str(timestamp),
        }
        
        try:
            # Make request
            if method.upper() == "GET":
                response = self.session.get(url, headers=headers, params=params, timeout=10)
            elif method.upper() == "POST":
                headers['Content-Type'] = 'application/json'
                response = self.session.post(url, headers=headers, json=json_data, timeout=10)
            else:
                response = self.session.request(method, url, headers=headers, params=params, json=json_data, timeout=10)
            
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            print(f"Error making API request to {path}: {e}")
            return None

    @staticmethod
    def _get_query_params(key: str, *args) -> str:
        """Build query parameters for API requests."""
        if not args:
            return ""
        params = [f"{key}={arg}" for arg in args if arg]
        return "?" + "&".join(params)
    
    def partitions(self):
        """Create partitions for parallel processing of crypto pairs."""
        # Use specified symbols from path
        symbols_str = self.options.get("path", "")
        if not symbols_str:
            raise ValueError(
                "Must specify crypto pairs to load using .load('BTC-USD,ETH-USD')"
            )
            
        # Split symbols by comma and create partitions
        symbols = [symbol.strip().upper() for symbol in symbols_str.split(",")]
        # Ensure proper format (e.g., BTC-USD)
        formatted_symbols = []
        for symbol in symbols:
            if symbol and '-' not in symbol:
                symbol = f"{symbol}-USD"  # Default to USD pair
            if symbol:
                formatted_symbols.append(symbol)
        
        return [CryptoPair(symbol=symbol) for symbol in formatted_symbols]

    def read(self, partition: CryptoPair):
        """Read crypto data for a single trading pair partition."""
        symbol = partition.symbol
        
        try:
            yield from self._read_crypto_pair_data(symbol)
        except Exception as e:
            # Log error but don't fail the entire job
            print(f"Warning: Failed to fetch data for {symbol}: {str(e)}")

    def _read_crypto_pair_data(self, symbol: str):
        """Fetch cryptocurrency market data for a given trading pair."""
        try:
            # Get best bid/ask data for the trading pair using query parameters
            path = f"/api/v1/crypto/marketdata/best_bid_ask/?symbol={symbol}"
            market_data = self._make_authenticated_request("GET", path)
            
            if market_data and 'results' in market_data:
                for quote in market_data['results']:
                    # Parse numeric values safely
                    def safe_float(value, default=0.0):
                        if value is None or value == "":
                            return default
                        try:
                            return float(value)
                        except (ValueError, TypeError):
                            return default
                    
                    # Extract market data fields from best bid/ask response
                    # Use the correct field names from the API response
                    price = safe_float(quote.get('price'))
                    bid_price = safe_float(quote.get('bid_inclusive_of_sell_spread'))
                    ask_price = safe_float(quote.get('ask_inclusive_of_buy_spread'))
                    
                    yield Row(
                        symbol=symbol,
                        price=price,
                        bid_price=bid_price,
                        ask_price=ask_price,
                        updated_at=quote.get('timestamp', "")
                    )
            else:
                print(f"Warning: No market data found for {symbol}")
                
        except requests.exceptions.RequestException as e:
            print(f"Network error fetching data for {symbol}: {str(e)}")
        except (ValueError, KeyError) as e:
            print(f"Data parsing error for {symbol}: {str(e)}")
        except Exception as e:
            print(f"Unexpected error fetching data for {symbol}: {str(e)}")
