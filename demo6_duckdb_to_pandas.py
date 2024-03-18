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
print(df)
df_agg = df.explode("abilities").groupby("abilities").agg(count=("id", "count")).sort_values("count", ascending=False)
print(df_agg)
