import duckdb

con = duckdb.connect("../dev.duckdb")

con.execute("CREATE SCHEMA IF NOT EXISTS raw")
print("Schemas available: raw")

con.close()