import zipfile, pandas as pd
from db import get_connection, init_db

def ingest(zip_path="store-monitoring-data.zip"):
    init_db()
    conn = get_connection()

    with zipfile.ZipFile(zip_path) as z:
        with z.open("store_status.csv") as f:
            pd.read_csv(f).to_sql("store_status", conn, if_exists="replace", index=False)
        with z.open("menu_hours.csv") as f:
            pd.read_csv(f).to_sql("business_hours", conn, if_exists="replace", index=False)
        with z.open("timezones.csv") as f:
            pd.read_csv(f).to_sql("store_timezone", conn, if_exists="replace", index=False)
    
    conn.close()
    print("Data ingested into store.db")

if __name__ == "__main__":
    ingest()
