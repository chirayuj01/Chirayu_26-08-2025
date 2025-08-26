import sqlite3

DB_PATH = "store.db"

def get_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_connection()
    cur = conn.cursor()
    cur.executescript("""
    CREATE TABLE IF NOT EXISTS store_status(
        store_id TEXT,
        timestamp_utc TEXT,
        status TEXT
    );

    CREATE TABLE IF NOT EXISTS business_hours(
        store_id TEXT,
        day_of_week INT,
        start_time_local TEXT,
        end_time_local TEXT
    );

    CREATE TABLE IF NOT EXISTS store_timezone(
        store_id TEXT,
        timezone_str TEXT
    );

    CREATE TABLE IF NOT EXISTS reports(
        report_id TEXT PRIMARY KEY,
        status TEXT,
        csv_path TEXT
    );
    """)
    conn.commit()
    conn.close()
