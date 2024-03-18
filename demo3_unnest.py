import duckdb

duckdb.sql("""
    CREATE TABLE pokemon AS
    SELECT unnest(results) AS pokemon
    FROM read_json_auto('https://pokeapi.co/api/v2/pokemon?limit=1000');

    SELECT
        pokemon.name,
        pokemon.url
    FROM pokemon;
""").show()
