import duckdb
import os

con = duckdb.connect(os.path.join(os.path.dirname(__file__), "..", "dev.duckdb"))

# Check what schemas exist using information_schema
result = con.execute("SELECT schema_name FROM information_schema.schemata").fetchdf()
print("Schemas:")
print(result)

print("\n" + "="*50 + "\n")

# Check all tables
result = con.execute("SHOW TABLES").fetchdf()
print("All tables:")
print(result)

con.close()