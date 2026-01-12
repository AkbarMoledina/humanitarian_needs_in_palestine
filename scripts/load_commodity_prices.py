import duckdb
import pandas as pd
import datetime
import os

# File path
file_path = r"C:\Users\akbar\PycharmProjects\humanitarian_needs_in_palestine\data\Prices of basic commodities in Gaza\commodity-prices-in-gaza.xlsx"

if not os.path.exists(file_path):
    raise FileNotFoundError(f"The file {file_path} does not exist.")

df = pd.read_excel(file_path, header=1)
print(f"Original shape: {df.shape}")

# Drop first column and Arabic columns
df = df.drop(df.columns[0], axis=1)
df = df.drop(['commodity name (arabic)', 'amount (arabic)'], axis=1, errors='ignore')

# Get date columns
date_cols = [col for col in df.columns if isinstance(col, (pd.Timestamp, datetime.datetime))]

# Melt the dataframe
id_vars = ['commodity name (english)', 'amount (english)', 'average price before 7 October 2023', 'average price after 7 October 2023']
df_long = df.melt(
    id_vars=id_vars,
    value_vars=date_cols,
    var_name='price_date',
    value_name='price'
)

# Clean data
df_long['price_date'] = pd.to_datetime(df_long['price_date'])
df_long['price'] = pd.to_numeric(df_long['price'], errors='coerce')

# Remove rows with null prices
df_long = df_long.dropna(subset=['price'])

# Sort by commodity and date
df_long = df_long.sort_values(['commodity name (english)', 'price_date'])

print(f"Transformed shape: {df_long.shape}\n")

# Connect to DuckDB
con = duckdb.connect("../dev.duckdb")

# Create table
con.execute("CREATE OR REPLACE TABLE raw.commodity_prices_gaza AS SELECT * FROM df_long")

# Show sample
sample = con.execute("SELECT * FROM raw.commodity_prices_gaza LIMIT 10").df()
print(sample)

con.close()