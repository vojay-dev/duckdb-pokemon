import duckdb
import requests
from duckdb.typing import VARCHAR


def get(url):
    return requests.get(url).text


duckdb.create_function("get", get, [VARCHAR], VARCHAR)
duckdb.sql("""
    CREATE TABLE pokemon AS
    SELECT unnest(results) AS pokemon
    FROM read_json_auto('https://pokeapi.co/api/v2/pokemon?limit=10');

    WITH base AS (
        SELECT
            pokemon.name AS name,
            json(get(pokemon.url)) AS details
        FROM pokemon
    )
    SELECT *
    FROM base;
""").show()
