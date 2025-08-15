import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, to_timestamp, avg

# env vakues
GCS_BUCKET_NAME = os.getenv("GCP_BUCKET_NAME")
BIGQUERY_DATASET = os.getenv("BIGQUERY_DATASET")
BIGQUERY_TABLE = os.getenv("BIGQUERY_TABLE")
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
TEMP_BUCKET = os.getenv("TEMP_BUCKET")


# spark 

def main():
    
    spark = SparkSession.builder \
        .appName("gcs to bigquery") \
        .getOrCreate()


    base_path = f"gs://{GCS_BUCKET_NAME}/"
    
# extract 

    orders = spark.read.csv(f"{base_path}olist_orders_dataset.csv", header=True, inferSchema=True)
    order_items = spark.read.csv(f"{base_path}olist_order_items_dataset.csv", header=True, inferSchema=True)
    customers = spark.read.csv(f"{base_path}olist_customers_dataset.csv", header=True, inferSchema=True)
    products = spark.read.csv(f"{base_path}olist_products_dataset.csv", header=True, inferSchema=True)
    payments = spark.read.csv(f"{base_path}olist_order_payments_dataset.csv", header=True, inferSchema=True)
    reviews = spark.read.csv(f"{base_path}olist_order_reviews_dataset.csv", header=True, inferSchema=True)



# transform

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


# load 
    spark.conf.set("temporary-gcs-bucket", TEMP_BUCKET)

    final_df.write \
        .format("bigquery") \
        .option("table", f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE}") \
        .mode("overwrite") \
        .save()

    spark.stop()


if __name__ == "__main__":
    main()
        