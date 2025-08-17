# E-commerce ETL Pipeline Project

A modern data engineering project that processes e-commerce data using Apache Spark and Google Cloud Platform.

## Project Architecture

### Project Structure
```
ecommerce-etl-pipeline/
├── spark_app/                    # Spark ETL 
│   ├── src/                      # Source code
│   │   ├── etl_job.py              # Main ETL job
│   ├── Dockerfile                   # Container configuration
│   ├── requirements.txt             # Python dependencies
│   └── .env_spark                   # Environment variables
├── config/                       # Configuration files (future)
│   └── upload_to_gcs.py            # GCS upload utility
├── notebooks/                    # Jupyter notebooks
├── venv/                         # Python virtual environment
└── README.md                        # Project documentation
```

## ETL Pipeline Architecture

### 1. **Data Sources (Extract)**
- **GCS Bucket**: `e_commerce_etl_bucket`
- **Raw Data Files**:
  - `olist_orders_dataset.csv`
  - `olist_order_items_dataset.csv`
  - `olist_customers_dataset.csv`
  - `olist_products_dataset.csv`
  - `olist_order_payments_dataset.csv`
  - `olist_order_reviews_dataset.csv`

### 2. **Data Processing (Transform)**
- **Apache Spark 3.5.0** with Python 3
- **Data Transformations**:
  - Join operations (orders, items, customers, products)
  - Aggregation (average review scores, total payments)
  - Data filtering (delivered orders only)
  - Schema transformation

### 3. **Data Destination (Load)**
- **Output**: Processed data saved to GCS
- **Format**: CSV with headers


## Container Architecture

### **Docker Configuration**
- **Base Image**: `apache/spark:3.5.0-python3`
- **GCS Connector**: Hadoop GCS connector JAR
- **Python Packages**: pyspark, google-cloud-storage, google-cloud-bigquery, pandas
- **Environment**: Local Spark master with single node


## Technology Stack

### **Core Technologies**
- **Apache Spark 3.5.0** - data processing
- **Python 3.8** - for scripts
- **Docker** - Containerization
- **Google Cloud Storage** - Data storage
- **Google Cloud BigQuery** - Data warehouse

### **Dependencies**
- **pyspark** - Spark Python API
- **google-cloud-storage** - GCS client library
- **google-cloud-bigquery** - BigQuery client library

## Deployment Architecture

### **Local Development**
1. **Environment Setup**: Python virtual environment
2. **Docker Build**: `docker build -t ecommerce-etl .`
3. **Container Run**: `docker run --rm -it ecommerce-etl`

### **Production Ready Features**
- **Environment Variables**: Configurable via .env files
- **Google Cloud Authentication**: Service account credentials
- **Error Handling**: Comprehensive error checking
- **Logging**: Structured logging with Spark

## Data Flow Architecture

```
Raw CSV Files (GCS)
        ↓
   Apache Spark ETL
        ↓
   Data Transformations
        ↓
   Processed Data (GCS)
        ↓
   Future: BigQuery Load
```

