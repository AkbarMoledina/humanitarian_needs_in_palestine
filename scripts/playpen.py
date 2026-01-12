import duckdb

con = duckdb.connect('../dev.duckdb')

print(con.execute("DESCRIBE requirements_and_funding").df())

df = con.execute("""
SELECT * FROM requirements_and_funding
""").df()

print(df)