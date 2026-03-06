import pandas as pd
import psycopg2
from pathlib import Path

# -----------------------------
# File paths
# -----------------------------
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
REPORTS_DIR = BASE_DIR / "reports"

RAW_FILE = DATA_DIR / "raw_trip_data.csv"
CLEAN_FILE = DATA_DIR / "cleaned_trip_data.csv"
DAILY_REPORT_FILE = REPORTS_DIR / "daily_revenue_report.csv"

# -----------------------------
# Load raw data
# -----------------------------
print("Loading raw taxi trip data...")
df = pd.read_csv(RAW_FILE)

print(f"Original dataset shape: {df.shape}")

# -----------------------------
# Select only needed columns
# -----------------------------
required_columns = [
    "VendorID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "pickup_longitude",
    "pickup_latitude",
    "dropoff_longitude",
    "dropoff_latitude",
    "payment_type",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount"
]

df = df[required_columns].copy()

# -----------------------------
# Data cleaning
# -----------------------------
print("Cleaning data...")

df = df.dropna()

df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"], errors="coerce")
df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"], errors="coerce")

df = df.dropna(subset=["tpep_pickup_datetime", "tpep_dropoff_datetime"])

# Keep only valid business records
df = df[df["trip_distance"] > 0]
df = df[df["total_amount"] > 0]
df = df[df["fare_amount"] > 0]
df = df[df["passenger_count"] > 0]

# Feature engineering
df["trip_duration_min"] = (
    df["tpep_dropoff_datetime"] - df["tpep_pickup_datetime"]
).dt.total_seconds() / 60

df = df[df["trip_duration_min"] > 0]

df["pickup_hour"] = df["tpep_pickup_datetime"].dt.hour
df["pickup_day_name"] = df["tpep_pickup_datetime"].dt.day_name()
df["pickup_date"] = df["tpep_pickup_datetime"].dt.date

print(f"Cleaned dataset shape: {df.shape}")

# -----------------------------
# Save cleaned dataset
# -----------------------------
df.to_csv(CLEAN_FILE, index=False)
print(f"Cleaned dataset saved to: {CLEAN_FILE}")

# -----------------------------
# Create daily revenue report
# -----------------------------
daily_revenue = (
    df.groupby("pickup_date")["total_amount"]
    .sum()
    .reset_index()
    .rename(columns={"total_amount": "daily_revenue"})
)

daily_revenue.to_csv(DAILY_REPORT_FILE, index=False)
print(f"Daily revenue report saved to: {DAILY_REPORT_FILE}")

# -----------------------------
# Load data into PostgreSQL
# -----------------------------
print("Connecting to PostgreSQL...")

conn = psycopg2.connect(
    database="taxi_analytics",
    user="postgres",
    password="Rishabh@1210",
    host="localhost",
    port="5432"
)

cursor = conn.cursor()

print("Clearing old table data...")
cursor.execute("TRUNCATE TABLE taxi_trips;")

print("Inserting cleaned data into taxi_trips...")

insert_query = """
    INSERT INTO taxi_trips (
        vendor_id,
        pickup_datetime,
        dropoff_datetime,
        passenger_count,
        trip_distance,
        pickup_longitude,
        pickup_latitude,
        dropoff_longitude,
        dropoff_latitude,
        payment_type,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        total_amount
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

rows = [
    (
        int(row["VendorID"]),
        row["tpep_pickup_datetime"].to_pydatetime(),
        row["tpep_dropoff_datetime"].to_pydatetime(),
        int(row["passenger_count"]),
        float(row["trip_distance"]),
        float(row["pickup_longitude"]),
        float(row["pickup_latitude"]),
        float(row["dropoff_longitude"]),
        float(row["dropoff_latitude"]),
        int(row["payment_type"]),
        float(row["fare_amount"]),
        float(row["extra"]),
        float(row["mta_tax"]),
        float(row["tip_amount"]),
        float(row["tolls_amount"]),
        float(row["improvement_surcharge"]),
        float(row["total_amount"])
    )
    for _, row in df.iterrows()
]

print("Rows prepared for insert:", len(rows))

cursor.executemany(insert_query, rows)

print("Rows inserted according to cursor:", cursor.rowcount)

conn.commit()

cursor.execute("SELECT COUNT(*) FROM taxi_trips;")
count = cursor.fetchone()[0]

print("Row count seen from Python after commit:", count)

cursor.close()
conn.close()

print("Data pipeline executed successfully.")