import pandas as pd
import yaml
import logging
from sqlalchemy import create_engine, text
from pathlib import Path
import os
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DatabaseLoader:
    def __init__(self, config_path='config/db_config.yaml'):
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)

        db = self.config['database']
        db['host'] = os.getenv('DB_HOST', db['host'])
        db['port'] = os.getenv('DB_PORT', db['port'])
        db['name'] = os.getenv('DB_NAME', db['name'])
        db['user'] = os.getenv('DB_USER', db['user'])
        db['password'] = os.getenv('DB_PASSWORD', db['password'])

        if not db['password']:
            raise ValueError("Database password is required.")

        logger.info(f"Connecting to: {db['host']}:{db['port']}/{db['name']} as {db['user']}")
        connection_string = f"postgresql://{db['user']}:{db['password']}@{db['host']}:{db['port']}/{db['name']}"
        self.engine = create_engine(connection_string)
        logger.info("Database connection established.")

    def create_tables(self):
        try:
            sql = """
            CREATE TABLE IF NOT EXISTS bike_trips (
                id SERIAL PRIMARY KEY,
                ride_id VARCHAR(255) UNIQUE NOT NULL,
                rideable_type VARCHAR(50),
                started_at TIMESTAMP,
                ended_at TIMESTAMP,
                start_station_name VARCHAR(255),
                start_station_id VARCHAR(100),
                end_station_name VARCHAR(255),
                end_station_id VARCHAR(100),
                start_lat DOUBLE PRECISION,
                start_lng DOUBLE PRECISION,
                end_lat DOUBLE PRECISION,
                end_lng DOUBLE PRECISION,
                member_casual VARCHAR(20),
                trip_duration_minutes DOUBLE PRECISION,
                hour INTEGER,
                day_of_week VARCHAR(20),
                month INTEGER,
                year INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
            with self.engine.begin() as conn:
                conn.execute(text(sql))
            logger.info("Tables created.")
        except Exception as e:
            logger.error(f"Create tables failed: {e}")
            raise

    def load_data(self, csv_path):
        try:
            logger.info(f"Loading {csv_path}")
            df = pd.read_csv(csv_path)
            df['created_at'] = datetime.now()
            df['updated_at'] = datetime.now()
            df.to_sql('bike_trips', self.engine, if_exists='append', index=False, method='multi')
            logger.info(f"Inserted {len(df)} rows.")
        except Exception as e:
            logger.error(f"Load failed: {e}")
            raise


def main():
    try:
        loader = DatabaseLoader()
        loader.create_tables()

        processed_path = Path("data/processed")
        files = list(processed_path.glob("*.csv"))
        if not files:
            logger.warning("No CSV found.")
            return

        latest_file = max(files, key=lambda f: f.stat().st_mtime)
        loader.load_data(latest_file)
        logger.info("ETL done.")
    except Exception as e:
        logger.error(f"ETL failed: {e}")


if __name__ == "__main__":
    main()
