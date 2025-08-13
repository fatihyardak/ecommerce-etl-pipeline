import os 
from pathlib import Path
from google.cloud import storage

GCS_BUCKET_NAME = "e_commerce_etl_bucket"
LOCAL_DATA_PATH = Path(__file__).parent.parent / "data"

def upload_to_gcs():
    "uploads files from /data to gcs"

    try: 
        storage_client = storage.Client()
        bucket  = storage_client.bucket(GCS_BUCKET_NAME)
        print("Uploading files to GCS bucket")
    
    except Exception as e:
        print(f"Error uploading files to GCS: {e}")

    file_count = 0 
    for local_file in os.listdir(LOCAL_DATA_PATH):
        local_file_path = os.path.join(LOCAL_DATA_PATH, local_file)

        blob = bucket.blob(local_file)

        try: 
            blob.upload_from_filename(local_file_path)
            print(f"Uploaded {local_file} to GCS")
            file_count += 1

        except Exception as e:
            print(f"Error uploading {local_file} to GCS: {e}")


if __name__ == "__main__":
    upload_to_gcs()