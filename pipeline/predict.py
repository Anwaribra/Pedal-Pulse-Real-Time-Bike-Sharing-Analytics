from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.regression import RandomForestRegressionModel
import yaml

def create_spark_session():
    return SparkSession.builder \
        .appName("BikeSharingPredictions") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.2.18") \
        .getOrCreate()

def load_config():
    with open("config/db_config.yaml") as f:
        return yaml.safe_load(f)

def prepare_features(df):
    # Prepare features for prediction
    numeric_features = ["hour_of_day", "day_of_week", "month", "distance"]
    
    assembler = VectorAssembler(
        inputCols=numeric_features,
        outputCol="features"
    )
    df_assembled = assembler.transform(df)
    
    scaler = StandardScaler(
        inputCol="features",
        outputCol="scaled_features"
    )
    df_scaled = scaler.fit(df_assembled).transform(df_assembled)
    
    return df_scaled

def load_model():
    return RandomForestRegressionModel.load("models/duration_prediction_model")

def generate_predictions(spark, db_config):
    # load recent data from PostgreSQL
    df = spark.read \
        .format("jdbc") \
        .option("url", f"jdbc:postgresql://{db_config['host']}/{db_config['database']}") \
        .option("dbtable", "processed_rides") \
        .option("user", db_config["user"]) \
        .option("password", db_config["password"]) \
        .load()
    
    df_features = prepare_features(df)
  
    model = load_model()
    predictions = model.transform(df_features)
    
    # relevant columns and predictions
    results = predictions.select(
        "ride_id",
        "start_station_id",
        "end_station_id",
        "ride_duration",
        col("prediction").alias("predicted_duration")
    )
    
    # save predictions to database
    results.write \
        .format("jdbc") \
        .option("url", f"jdbc:postgresql://{db_config['host']}/{db_config['database']}") \
        .option("dbtable", "ride_predictions") \
        .option("user", db_config["user"]) \
        .option("password", db_config["password"]) \
        .mode("append") \
        .save()

def main():
    spark = create_spark_session()
    config = load_config()
    generate_predictions(spark, config)
    
    spark.stop()

if __name__ == "__main__":
    main() 