import requests
import math

from pyspark.sql import Row
from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition

class SWAPIDataSource(DataSource):
    """
    Implementing the SWAPI API as to https://swapi.dev/documentation

    Name: `SWAPI`

    Schema (depends on the called resource): 

        "people": 'birth_year string, created string, edited string, eye_color string, films array<string>, 
        gender string, hair_color string, height string, homeworld string, mass string, name string, skin_color string, 
        species array<string>, starships array<string>, url string, vehicles array<string>',

        "films": 'characters array<string>, created string, director string, edited string, episode_id bigint, 
        opening_crawl string, planets array<string>, producer string, release_date string, species array<string>, 
        starships array<string>, title string, url string, vehicles array<string>',

        "starships": 'MGLT string, cargo_capacity string, consumables string, cost_in_credits string, created string, 
        crew string, edited string, films array<string>, hyperdrive_rating string, length string, manufacturer string, 
        max_atmosphering_speed string, model string, name string, passengers string, pilots array<string>, starship_class string, url string',

        "vehicles": 'cargo_capacity string, consumables string, cost_in_credits string, created string, crew string, 
        edited string, films array<string>, length string, manufacturer string, max_atmosphering_speed string, model string, 
        name string, passengers string, pilots array<string>, url string, vehicle_class string',

        "species": 'average_height string, average_lifespan string, classification string, created string, designation string, 
        edited string, eye_colors string, films array<string>, hair_colors string, homeworld string, language string, name string, 
        people array<string>, skin_colors string, url string',

        "planets": 'climate string, created string, diameter string, edited string, films array<string>, gravity string, name string, 
        orbital_period string, population string, residents array<string>, rotation_period string, surface_water string, terrain string, url string'

    Examples
    --------
    Register the data source.

    >>> from pyspark_datasources import SWAPIDataSource
    >>> spark.dataSource.register(SWAPIDataSource)

    Load data from the availble resources "people", "films", "starships", "vehicles", "species", "planets"

    >>> spark.read.format("SWAPI").load("planets").show()
 
    +---------+--------------------+--------+--------------------+--------------------+----------+--------+---+
    |  climate|             created|diameter|              edited|               films|   gravity|    name|...|
    +---------+--------------------+--------+--------------------+--------------------+----------+--------+---+
    |     arid|2014-12-09T13:50:...|   10465|2014-12-20T20:58:...|[https://swapi.de...|1 standard|Tatooine|...|
    |...      |...                 |...     |...                 |                 ...|...       |...     |...|
    +---------+--------------------+--------+--------------------+--------------------+----------+--------+---+

    """

    @classmethod
    def name(self):
        return "SWAPI"
    
    def schema(self):
        if self.options["path"] not in ["people", "films", "starships", "vehicles", "species", "planets"]:
            raise Exception(f"Assure that only values in ['people', 'films', 'starships', 'vehicles', 'species', 'planets'] are provided")
        
        schemas = {
            "people": 'birth_year string, created string, edited string, eye_color string, films array<string>, gender string, hair_color string, height string, homeworld string, mass string, name string, skin_color string, species array<string>, starships array<string>, url string, vehicles array<string>',

            "films": 'characters array<string>, created string, director string, edited string, episode_id bigint, opening_crawl string, planets array<string>, producer string, release_date string, species array<string>, starships array<string>, title string, url string, vehicles array<string>',

            "starships": 'MGLT string, cargo_capacity string, consumables string, cost_in_credits string, created string, crew string, edited string, films array<string>, hyperdrive_rating string, length string, manufacturer string, max_atmosphering_speed string, model string, name string, passengers string, pilots array<string>, starship_class string, url string',

            "vehicles": 'cargo_capacity string, consumables string, cost_in_credits string, created string, crew string, edited string, films array<string>, length string, manufacturer string, max_atmosphering_speed string, model string, name string, passengers string, pilots array<string>, url string, vehicle_class string',

            "species": 'average_height string, average_lifespan string, classification string, created string, designation string, edited string, eye_colors string, films array<string>, hair_colors string, homeworld string, language string, name string, people array<string>, skin_colors string, url string',

            "planets": 'climate string, created string, diameter string, edited string, films array<string>, gravity string, name string, orbital_period string, population string, residents array<string>, rotation_period string, surface_water string, terrain string, url string'
        }

        return schemas[self.options["path"]]

    def reader(self, schema):
        return SWAPIDataSourceReader(self.options)


class SWAPIDataSourceReader(DataSourceReader):
    def __init__(self, options):
        self.resource = options["path"]

    def partitions(self):
        query = f"https://swapi.dev/api/{self.resource}/"
        page_size = 10
        total_elements = int(requests.get(query).json()["count"])
        no_pages = math.ceil(total_elements / page_size)
        return [InputPartition(i) for i in range(1, no_pages + 1)]

    def read(self, partition):
        query = f"https://swapi.dev/api/{self.resource}/?page={str(partition.value)}"
        req = requests.get(query)
        data = req.json()["results"]
        for d in data:
            yield Row(**d)

spark.dataSource.register(SWAPIDataSource)
