import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, to_timestamp, avg

# .env dosyasından ortam değişkenlerini okuyoruz
GCS_BUCKET_NAME = os.getenv("GCP_GCS_BUCKET_NAME")
BIGQUERY_DATASET = os.getenv("BIGQUERY_DATASET")
BIGQUERY_TABLE = os.getenv("BIGQUERY_TABLE")
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
TEMP_BUCKET = os.getenv("GCP_GCS_TEMP_BUCKET")

def main():
    """
    Extract - Transform - Load
    """
    if not GCS_BUCKET_NAME:
        raise ValueError("GCP_GCS_BUCKET_NAME environment variable is not set")
    
    spark = SparkSession.builder \
        .appName("GCS to BigQuery ETL Job") \
        .getOrCreate()
    

    spark.sparkContext.setLogLevel("WARN")
    print("Spark session started")

    # extract
    base_path = f"gs://{GCS_BUCKET_NAME}/"
    
    print("CSV file reading")
    orders = spark.read.csv(f"{base_path}olist_orders_dataset.csv", header=True, inferSchema=True)
    order_items = spark.read.csv(f"{base_path}olist_order_items_dataset.csv", header=True, inferSchema=True)
    customers = spark.read.csv(f"{base_path}olist_customers_dataset.csv", header=True, inferSchema=True)
    products = spark.read.csv(f"{base_path}olist_products_dataset.csv", header=True, inferSchema=True)
    payments = spark.read.csv(f"{base_path}olist_order_payments_dataset.csv", header=True, inferSchema=True)
    reviews = spark.read.csv(f"{base_path}olist_order_reviews_dataset.csv", header=True, inferSchema=True)
    print("Tüm dosyalar başarıyla DataFrame'lere yüklendi.")

    # transform
    print("transform starting")
    order_details = orders.join(order_items, "order_id", "inner")
    order_details = order_details.join(customers, "customer_id", "inner")
    order_details = order_details.join(products, "product_id", "inner")
    
    avg_reviews = reviews.groupBy("order_id").agg(avg("review_score").alias("avg_review_score"))
    order_details = order_details.join(avg_reviews, "order_id", "left_outer")

    total_payments = payments.groupBy("order_id").agg(sum("payment_value").alias("total_payment_value"))
    order_details = order_details.join(total_payments, "order_id", "left_outer")

    final_df = order_details.select(
        col("order_id"),
        col("customer_unique_id"),
        col("customer_city"),
        col("customer_state"),
        to_timestamp("order_purchase_timestamp").alias("purchase_timestamp"),
        col("order_status"),
        col("product_id"),
        col("product_category_name"),
        col("price"),
        col("freight_value"),
        col("total_payment_value"),
        col("avg_review_score")
    )
    
    final_df = final_df.filter(col("order_status") == "delivered")
    print("Veri dönüştürme tamamlandı.")

    # load - CSV olarak GCS'ye kaydet (BigQuery connector yok)
    print(f"writing to GCS as CSV")

    spark.conf.set('temporaryGcsBucket', TEMP_BUCKET)

    
    final_df.write \
        .format("csv") \
        .option("header", "true") \
        .mode("overwrite") \
        .save(f"gs://{GCS_BUCKET_NAME}/processed_data/orders_summary")

    print(f"all steps completed successfully")
    print(f"Output location: gs://{GCS_BUCKET_NAME}/processed_data/orders_summary")
    
    spark.stop()


if __name__ == "__main__":
    main()