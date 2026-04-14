import duckdb
import pandas as pd
from pathlib import Path

# Load file
PROJECT_ROOT = Path(__file__).parent.parent
file_path = PROJECT_ROOT / "data" / "Gaza Supplies and Dispatch Tracking" / "Commodities Received.xlsx"

if not file_path.exists():
    raise FileNotFoundError(f"File not found: {file_path}")

df = pd.read_excel(file_path)
print(f"Original shape: {df.shape}\n")

# Connect to DuckDB and create table
con = duckdb.connect("../dev.duckdb")
con.execute("CREATE OR REPLACE TABLE raw.aid_received AS SELECT * FROM df")

sample = con.execute("SELECT * FROM raw.aid_received LIMIT 10").df()
print(sample)

con.close()