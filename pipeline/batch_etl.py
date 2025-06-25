from pathlib import Path
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

RAW_DATA_DIR = Path("data/raw")
PROCESSED_DATA_DIR = Path("data/processed")

def clean_trip_data(df):
    df = df.copy()
    
    def parse_datetime_robust(series):
        formats_to_try = [
            '%Y-%m-%d %H:%M:%S',
            '%Y-%m-%d %H:%M:%S.%f',
            '%Y-%m-%d %H:%M',
            '%Y-%m-%d',
            '%m/%d/%Y %H:%M:%S',
            '%m/%d/%Y %H:%M',
            '%d/%m/%Y %H:%M:%S',
            '%d/%m/%Y %H:%M'
        ]
        
        result = pd.Series([pd.NaT] * len(series), index=series.index)
        
        for fmt in formats_to_try:
            try:
                parsed = pd.to_datetime(series, format=fmt, errors='coerce')
                result = result.fillna(parsed)
            except:
                continue
        
        try:
            auto_parsed = pd.to_datetime(series, errors='coerce')
            result = result.fillna(auto_parsed)
        except:
            pass
        
        return result
    
    df['started_at'] = parse_datetime_robust(df['started_at'])
    df['ended_at'] = parse_datetime_robust(df['ended_at'])
    
    df = df.dropna(subset=['ride_id', 'started_at', 'ended_at', 'start_station_id', 'end_station_id'])
    
    df['trip_duration_minutes'] = (df['ended_at'] - df['started_at']).dt.total_seconds() / 60
    df = df[df['trip_duration_minutes'] > 0]
    
    df['hour'] = df['started_at'].dt.hour
    df['day_of_week'] = df['started_at'].dt.day_name()
    df['month'] = df['started_at'].dt.month
    df['year'] = df['started_at'].dt.year
    
    return df

def run_etl():
    PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)
    
    csv_files = list(RAW_DATA_DIR.glob("*.csv"))
    
    for file_path in csv_files:
        logger.info(f"Processing: {file_path.name}")
        df = pd.read_csv(file_path)
        df_cleaned = clean_trip_data(df)
        
        output_path = PROCESSED_DATA_DIR / f"cleaned_{file_path.name}"
        df_cleaned.to_csv(output_path, index=False)
        logger.info(f"Saved: {output_path}")

if __name__ == "__main__":
    run_etl()
