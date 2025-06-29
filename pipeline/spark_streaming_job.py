import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.regression import RandomForestRegressionModel
import yaml
from datetime import datetime
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class BikeSharingStreamProcessor:
    def __init__(self):
        """Initialize Spark streaming job"""
        self.spark = SparkSession.builder \
            .appName("BikeSharingProcessor") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
        self.model = RandomForestRegressionModel.load("models/duration_prediction_model")
        
        logger.info("Spark session and ML model initialized")
    
    def create_streaming_query(self):
        """Create the streaming query to process CSV data"""
        
        # Define schema for trip data
        trip_schema = StructType([
            StructField("ride_id", StringType(), True),
            StructField("rideable_type", StringType(), True),
            StructField("started_at", StringType(), True),
            StructField("ended_at", StringType(), True),
            StructField("start_station_name", StringType(), True),
            StructField("start_station_id", StringType(), True),
            StructField("end_station_name", StringType(), True),
            StructField("end_station_id", StringType(), True),
            StructField("start_lat", DoubleType(), True),
            StructField("start_lng", DoubleType(), True),
            StructField("end_lat", DoubleType(), True),
            StructField("end_lng", DoubleType(), True),
            StructField("member_casual", StringType(), True)
        ])
        
        # read from CSV files
        raw_df = self.spark.readStream \
            .format("csv") \
            .schema(trip_schema) \
            .option("header", "true") \
            .option("maxFilesPerTrigger", 1) \
            .load("data/raw/*.csv")
        
        # add processing timestamp and convert time fields
        processed_df = raw_df \
            .withColumn("processing_timestamp", current_timestamp()) \
            .withColumn("started_at_ts", to_timestamp("started_at")) \
            .withColumn("ended_at_ts", to_timestamp("ended_at"))
        
        # calculate trip duration and features
        trip_df = processed_df \
            .withColumn("trip_duration_minutes", 
                       (unix_timestamp("ended_at_ts") - unix_timestamp("started_at_ts")) / 60) \
            .withColumn("hour_of_day", hour("started_at_ts")) \
            .withColumn("day_of_week", dayofweek("started_at_ts")) \
            .withColumn("month", month("started_at_ts")) \
            .withColumn("distance", 
                sqrt(
                    pow(col("end_lat") - col("start_lat"), 2) + 
                    pow(col("end_lng") - col("start_lng"), 2)
                ) * 111)
        
        # Prepare features for ML
        feature_cols = ["hour_of_day", "day_of_week", "month", "distance"]
        assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
        df_assembled = assembler.transform(trip_df)
        
        # Scale features
        scaler = StandardScaler(inputCol="features", outputCol="scaled_features")
        scaler_model = scaler.fit(df_assembled)
        df_scaled = scaler_model.transform(df_assembled)
        
        # Generate predictions
        predictions = self.model.transform(df_scaled) \
            .withColumn("prediction_timestamp", current_timestamp())
        
        return predictions
    
    def create_station_aggregations(self, predictions_df):
        """Create station aggregations with predictions"""
        
        # Station-level aggregations
        station_agg = predictions_df \
            .groupBy("start_station_id", "start_station_name") \
            .agg(
                count("*").alias("total_trips"),
                avg("trip_duration_minutes").alias("actual_avg_duration"),
                avg("prediction").alias("predicted_avg_duration"),
                count(when(col("member_casual") == "member", True)).alias("member_trips"),
                count(when(col("member_casual") == "casual", True)).alias("casual_trips"),
                max("processing_timestamp").alias("last_update")
            )
        
        return station_agg
    
    def create_prediction_accuracy_metrics(self, predictions_df):
        """Calculate prediction accuracy metrics"""
        metrics_df = predictions_df \
            .withColumn("prediction_error", 
                       abs(col("prediction") - col("trip_duration_minutes"))) \
            .groupBy(window("processing_timestamp", "5 minutes")) \
            .agg(
                avg("prediction_error").alias("mean_absolute_error"),
                count("*").alias("total_predictions"),
                avg(
                    when(col("prediction_error") <= 5, 1)
                    .otherwise(0)
                ).alias("accuracy_within_5_mins")
            )
        
        return metrics_df
    
    def write_predictions_to_postgres(self, df, db_config):
        """Write predictions to PostgreSQL"""
        return df.writeStream \
            .foreachBatch(lambda batch_df, batch_id: 
                batch_df.write \
                    .format("jdbc") \
                    .option("url", f"jdbc:postgresql://{db_config['host']}/{db_config['database']}") \
                    .option("dbtable", "ride_predictions") \
                    .option("user", db_config["user"]) \
                    .option("password", db_config["password"]) \
                    .mode("append") \
                    .save()
            ) \
            .outputMode("update") \
            .trigger(processingTime="30 seconds")
    
    def run_streaming_job(self):
        """Run the main streaming job"""
        logger.info("Starting Spark streaming job")
        
        try:
            # Create streaming query with predictions
            predictions_df = self.create_streaming_query()
            
            # Create aggregations
            station_agg = self.create_station_aggregations(predictions_df)
            prediction_metrics = self.create_prediction_accuracy_metrics(predictions_df)
            
            # Load database config
            with open("config/db_config.yaml") as f:
                db_config = yaml.safe_load(f)
            
            # Start streaming queries
            queries = []
            
            # Console output for debugging
            console_query = predictions_df.writeStream \
                .outputMode("append") \
                .format("console") \
                .option("truncate", False) \
                .trigger(processingTime="10 seconds") \
                .start()
            queries.append(console_query)
            
            # Write predictions to PostgreSQL
            postgres_query = self.write_predictions_to_postgres(predictions_df, db_config)
            queries.append(postgres_query.start())
            
            # Memory tables for real-time monitoring
            station_query = station_agg.writeStream \
                .outputMode("complete") \
                .format("memory") \
                .queryName("station_predictions") \
                .trigger(processingTime="30 seconds") \
                .start()
            queries.append(station_query)
            
            metrics_query = prediction_metrics.writeStream \
                .outputMode("complete") \
                .format("memory") \
                .queryName("prediction_metrics") \
                .trigger(processingTime="30 seconds") \
                .start()
            queries.append(metrics_query)
            
            logger.info("All streaming queries started")
            
            # Wait for termination
            self.spark.streams.awaitAnyTermination()
            
        except KeyboardInterrupt:
            logger.info("Received interrupt signal. Stopping streaming job...")
        except Exception as e:
            logger.error(f"Error in streaming job: {e}")
        finally:
            self.spark.stop()
            logger.info("Spark session stopped")
    
    def query_real_time_metrics(self):
        """Query real-time prediction metrics"""
        try:
            # Query station predictions
            station_data = self.spark.sql("""
                SELECT 
                    start_station_name,
                    total_trips,
                    actual_avg_duration,
                    predicted_avg_duration,
                    member_trips,
                    casual_trips
                FROM station_predictions
                ORDER BY total_trips DESC
                LIMIT 10
            """)
            print("\nTop 10 Stations by Trip Volume:")
            station_data.show()
            
            # Query prediction metrics
            metrics_data = self.spark.sql("""
                SELECT 
                    window.start,
                    window.end,
                    mean_absolute_error,
                    total_predictions,
                    accuracy_within_5_mins
                FROM prediction_metrics
                ORDER BY window.end DESC
                LIMIT 5
            """)
            print("\nRecent Prediction Metrics:")
            metrics_data.show()
            
        except Exception as e:
            logger.error(f"Error querying real-time metrics: {e}")

def main():
    """Main function to run the Spark streaming job"""
    processor = BikeSharingStreamProcessor()
    processor.run_streaming_job()

if __name__ == "__main__":
    main()
