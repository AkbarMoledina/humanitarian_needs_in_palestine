import duckdb
import pandas as pd
import os

file_path = r"C:\Users\akbar\PycharmProjects\humanitarian_needs_in_palestine\data\Requirements and Funding Data/fts_requirements_funding_pse.csv"

if not os.path.exists(file_path):
    raise FileNotFoundError

df = pd.read_csv(file_path)

print(f"Original shape: {df.shape}\n")

con = duckdb.connect("../dev.duckdb")
con.execute("CREATE OR REPLACE TABLE raw.requirements_and_funding AS SELECT * FROM df")

sample = con.execute("SELECT * FROM raw.requirements_and_funding LIMIT 10").df()
print(sample)

con.close()