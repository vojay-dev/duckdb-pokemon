import duckdb

duckdb.sql("""
    CREATE TABLE pokemon AS
    SELECT *
    FROM read_json_auto('https://pokeapi.co/api/v2/pokemon?limit=1000');

    SELECT * FROM pokemon;
""").show()
