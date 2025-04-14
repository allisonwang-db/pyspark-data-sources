import json
import requests

from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType


class PokemonDataSource(DataSource):
    """
    A data source for reading Pokemon data from the PokeAPI (https://pokeapi.co/).
    
    Options
    -------
    
    - endpoint: The API endpoint to query. Default is "pokemon".
                Valid options include: "pokemon", "type", "ability", "berry", etc.
    - limit: Maximum number of results to return. Default is 20.
    - offset: Number of results to skip. Default is 0.
    
    Examples
    --------
    
    Register the data source:
    
    >>> from pyspark_datasources import PokemonDataSource
    >>> spark.dataSource.register(PokemonDataSource)
    
    Load Pokemon data:
    
    >>> df = spark.read.format("pokemon").option("limit", 10).load()
    >>> df.show()
    +----+-----------+--------+------+--------------------+
    |  id|       name|  height|weight|            abilities|
    +----+-----------+--------+------+--------------------+
    |   1|  bulbasaur|       7|    69|[overgrow, chloro...|
    |   2|    ivysaur|      10|   130|[overgrow, chloro...|
    |   3|   venusaur|      20|  1000|[overgrow, chloro...|
    |   4| charmander|       6|    85|[blaze, solar-power]|
    |   5| charmeleon|      11|   190|[blaze, solar-power]|
    |   6|  charizard|      17|   905|[blaze, solar-power]|
    |   7|   squirtle|       5|    90|[torrent, rain-dish]|
    |   8|  wartortle|      10|   225|[torrent, rain-dish]|
    |   9|  blastoise|      16|   855|[torrent, rain-dish]|
    |  10|  caterpie |       3|    29|        [shield-dust]|
    +----+-----------+--------+------+--------------------+
    
    Load specific Pokemon types:
    
    >>> df = spark.read.format("pokemon").option("endpoint", "type").load()
    >>> df.show()
    """
    
    @classmethod
    def name(cls) -> str:
        return "pokemon"
    
    def schema(self) -> str:
        if self.options.get("endpoint", "pokemon") == "pokemon":
            return (
                "id integer, name string, height integer, "
                "weight integer, abilities array<string>"
            )
        elif self.options.get("endpoint") == "type":
            return "id integer, name string, pokemon array<string>"
        else:
            # Generic schema for other endpoints
            return "id integer, name string, details string"
    
    def reader(self, schema):
        return PokemonDataReader(schema, self.options)


class PokemonPartition(InputPartition):
    def __init__(self, endpoint, limit, offset):
        self.endpoint = endpoint
        self.limit = limit
        self.offset = offset


class PokemonDataReader(DataSourceReader):
    def __init__(self, schema: StructType, options: dict):
        self.schema = schema
        self.options = options
        self.endpoint = options.get("endpoint", "pokemon")
        self.limit = int(options.get("limit", 20))
        self.offset = int(options.get("offset", 0))
        self.base_url = "https://pokeapi.co/api/v2"
        
    def partitions(self):
        # Create a single partition for simplicity
        return [PokemonPartition(self.endpoint, self.limit, self.offset)]
    
    def read(self, partition: PokemonPartition):
        session = requests.Session()
        
        # Fetch list of resources
        url = f"{self.base_url}/{partition.endpoint}?limit={partition.limit}&offset={partition.offset}"
        response = session.get(url)
        response.raise_for_status()
        results = response.json()["results"]
        
        # Process based on endpoint type
        if partition.endpoint == "pokemon":
            for result in results:
                pokemon_data = self._fetch_pokemon(result["url"], session)
                abilities = [ability["ability"]["name"] for ability in pokemon_data["abilities"]]
                
                yield (
                    pokemon_data["id"],
                    pokemon_data["name"],
                    pokemon_data["height"],
                    pokemon_data["weight"],
                    abilities
                )
        
        elif partition.endpoint == "type":
            for result in results:
                type_data = self._fetch_resource(result["url"], session)
                pokemon_names = [pokemon["pokemon"]["name"] for pokemon in type_data["pokemon"]]
                
                yield (
                    type_data["id"],
                    type_data["name"],
                    pokemon_names
                )
        
        else:
            # Generic handler for other endpoints
            for result in results:
                resource_data = self._fetch_resource(result["url"], session)
                
                yield (
                    resource_data.get("id", 0),
                    resource_data.get("name", ""),
                    json.dumps(resource_data)
                )
    
    @staticmethod
    def _fetch_pokemon(url, session):
        """Fetch detailed Pokemon data"""
        response = session.get(url)
        response.raise_for_status()
        return response.json()
    
    @staticmethod
    def _fetch_resource(url, session):
        """Fetch any resource data"""
        response = session.get(url)
        response.raise_for_status()
        return response.json() 