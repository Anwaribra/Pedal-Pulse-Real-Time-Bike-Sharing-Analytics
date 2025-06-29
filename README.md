# Pedal Pulse - Bike Sharing Analytics 

Transforming bike-sharing data into actionable insights through advanced analytics and machine learning 


## Architecture

```
CSV Data → Spark Processing → PostgreSQL → ML Models → Dashboards
    ↓           ↓               ↓           ↓           ↓
Raw Files → ETL/Streaming → Data Warehouse → Predictions → Insights
```






##  Data Pipeline

1. **Data Ingestion**: Raw bike-sharing data from CSV files
2. **Data Cleaning**: Remove duplicates, handle missing values, validate data
3. **Data Transformation**: Calculate trip duration, extract time features
4. **Data Warehousing**: Store processed data in PostgreSQL
5. **ML Forecasting**: Train and deploy demand prediction models
6. **Visualization**: Create interactive dashboards 


##  Features

###  **Data Processing**
- **Batch ETL Pipeline**: Clean and transform historical bike-sharing data
- **Spark Streaming**: Real-time data processing from CSV files
- **Data Validation**: Automated quality checks and duplicate removal
- **PostgreSQL Integration**: Scalable data warehousing

### **Machine Learning**
- **Demand Forecasting**: Predict bike availability at stations
- **Trip Duration Prediction**: ML-powered trip time estimation
- **Real-time Predictions**: Live scoring with trained models
- **Model Performance Tracking**: Accuracy metrics and monitoring

###  **Analytics & Visualization**
- **Station Performance**: Utilization rates and trip patterns
- **User Behavior Analysis**: Member vs casual user insights
- **Seasonal Trends**: Time-based usage patterns

### **Workflow Management**
- **Apache Airflow**: Automated pipeline scheduling
- **Docker Deployment**: Containerized application stack
- **Error Handling**: Robust retry mechanisms and monitoring
- **Configuration Management**: Environment-based settings














