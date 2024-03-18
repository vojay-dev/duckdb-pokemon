import duckdb
import requests
from duckdb.typing import VARCHAR


def get(url):
    return requests.get(url).text


duckdb.create_function("get", get, [VARCHAR], VARCHAR)
df = duckdb.sql("""
    CREATE TABLE pokemon AS
    SELECT unnest(results) AS pokemon
    FROM read_json_auto('https://pokeapi.co/api/v2/pokemon?limit=10');

    WITH base AS (
        SELECT
            pokemon.name AS name,
            json(get(pokemon.url)) AS details
        FROM pokemon
    ), pokemon_details AS (
        SELECT
            details.id,
            name,
            details.abilities::STRUCT(ability STRUCT(name VARCHAR, url VARCHAR), is_hidden BOOLEAN, slot INTEGER)[] AS abilities,
            details.height,
            details.weight
        FROM base
    )
    SELECT
        id,
        name,
        [x.ability.name FOR x IN abilities] AS abilities,
        height,
        weight
    FROM pokemon_details;
""").df()

# Continue data wrangling in Pandas
df_agg = df.explode("abilities").groupby("abilities", as_index=False).agg(count=("id", "count"))

# Persist data
with duckdb.connect(database="pokemon.db") as conn:
    conn.sql("""
        DROP TABLE IF EXISTS pokemon_abilities;
        CREATE TABLE pokemon_abilities AS
        SELECT abilities AS ability, count
        FROM df_agg
        ORDER BY count DESC;
    """)
