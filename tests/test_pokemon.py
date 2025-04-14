import unittest
from unittest.mock import patch, MagicMock
import json

from pyspark_datasources.pokemon import PokemonDataSource, PokemonDataReader, PokemonPartition


class TestPokemonDataSource(unittest.TestCase):
    def test_name(self):
        self.assertEqual(PokemonDataSource.name(), "pokemon")
    
    def test_schema_pokemon(self):
        source = PokemonDataSource({"endpoint": "pokemon"})
        expected_schema = (
            "id integer, name string, height integer, "
            "weight integer, abilities array<string>"
        )
        self.assertEqual(source.schema(), expected_schema)
    
    def test_schema_type(self):
        source = PokemonDataSource({"endpoint": "type"})
        expected_schema = "id integer, name string, pokemon array<string>"
        self.assertEqual(source.schema(), expected_schema)
    
    def test_schema_other(self):
        source = PokemonDataSource({"endpoint": "ability"})
        expected_schema = "id integer, name string, details string"
        self.assertEqual(source.schema(), expected_schema)


class TestPokemonDataReader(unittest.TestCase):
    @patch('pyspark_datasources.pokemon.requests.Session')
    def test_read_pokemon(self, mock_session):
        # Mock response for the list endpoint
        mock_response_list = MagicMock()
        mock_response_list.json.return_value = {
            "results": [
                {"name": "bulbasaur", "url": "https://pokeapi.co/api/v2/pokemon/1/"}
            ]
        }
        
        # Mock response for the individual pokemon
        mock_response_pokemon = MagicMock()
        mock_response_pokemon.json.return_value = {
            "id": 1,
            "name": "bulbasaur",
            "height": 7,
            "weight": 69,
            "abilities": [
                {"ability": {"name": "overgrow"}},
                {"ability": {"name": "chlorophyll"}}
            ]
        }
        
        # Set up the session mock
        mock_session_instance = mock_session.return_value
        mock_session_instance.get.side_effect = [mock_response_list, mock_response_pokemon]
        
        # Create reader and partition
        schema = None  # Not used in the test
        reader = PokemonDataReader(schema, {"endpoint": "pokemon", "limit": 1})
        partition = PokemonPartition("pokemon", 1, 0)
        
        # Get results
        results = list(reader.read(partition))
        
        # Verify the results
        self.assertEqual(len(results), 1)
        pokemon = results[0]
        self.assertEqual(pokemon[0], 1)  # id
        self.assertEqual(pokemon[1], "bulbasaur")  # name
        self.assertEqual(pokemon[2], 7)  # height
        self.assertEqual(pokemon[3], 69)  # weight
        self.assertEqual(pokemon[4], ["overgrow", "chlorophyll"])  # abilities
        
        # Verify the correct URLs were called
        mock_session_instance.get.assert_any_call(
            "https://pokeapi.co/api/v2/pokemon?limit=1&offset=0"
        )
        mock_session_instance.get.assert_any_call(
            "https://pokeapi.co/api/v2/pokemon/1/"
        )


if __name__ == "__main__":
    unittest.main() 