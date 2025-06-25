from pathlib import Path
import pandas as pd

RAW_DATA_DIR = Path("data/raw")
PROCESSED_DATA_DIR = Path("data/processed")

def clean_trip_data(df):
    df['started_at'] = pd.to_datetime(df['started_at'], format='%Y-%m-%dT%H:%M:%S.%fZ', errors='coerce')
    df['ended_at'] = pd.to_datetime(df['ended_at'], format='%Y-%m-%dT%H:%M:%S.%fZ', errors='coerce')

    df = df.dropna(subset=['ride_id', 'started_at', 'ended_at', 'start_station_id', 'end_station_id']).copy()

    df['trip_duration_minutes'] = (df['ended_at'] - df['started_at']).dt.total_seconds() / 60
    df = df[df['trip_duration_minutes'] > 0]
    df['hour'] = df['started_at'].dt.hour
    df['day_of_week'] = df['started_at'].dt.day_name()
    return df

def run_etl():
    PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)

    for file_path in RAW_DATA_DIR.glob("*.csv"):
        print(f"Processing: {file_path.name}")
        try:
            df = pd.read_csv(file_path)
            df_cleaned = clean_trip_data(df)
            output_path = PROCESSED_DATA_DIR / f"cleaned_{file_path.name}"
            df_cleaned.to_csv(output_path, index=False)
            print(f"Saved: {output_path}")
        except Exception as e:
            print(f"Error {file_path.name}: {e}")

if __name__ == "__main__":
    run_etl()
