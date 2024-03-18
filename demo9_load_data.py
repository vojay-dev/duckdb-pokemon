import duckdb

# Load data
with duckdb.connect(database="pokemon.db") as conn:
    conn.sql("""
        SELECT * FROM pokemon_abilities;
    """).show()
