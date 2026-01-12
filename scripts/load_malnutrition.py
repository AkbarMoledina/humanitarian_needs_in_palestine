import duckdb
import pandas as pd
import os

file_path = r"C:\Users\akbar\PycharmProjects\humanitarian_needs_in_palestine\data\Malnutrition Prevalence\malnutrition-in-gaza-strip_may2025.xlsx"

if not os.path.exists(file_path):
    raise FileNotFoundError

df = pd.read_excel(file_path)
print(f"Original shape: {df.shape}\n")

con = duckdb.connect("../dev.duckdb")
con.execute("CREATE OR REPLACE TABLE raw.malnutrition_in_gaza AS SELECT * FROM df")

sample = con.execute("SELECT * FROM raw.malnutrition_in_gaza LIMIT 10").df()
print(sample)

con.close()