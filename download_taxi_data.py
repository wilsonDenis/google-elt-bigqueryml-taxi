import requests
import time
from datetime import datetime, UTC  # UTC importé directement ici
from google.cloud import storage
import logging
import io

# --- Configuration ---
PROJECT_ID = "advance-path-477219-e1"
BUCKET_NAME = f"{PROJECT_ID}-data-bucket"
GCS_FOLDER = "dataset/trips/"
GCS_LOG_FOLDER = "from-git/logs/"

# --- Initialisation du client Google Cloud Storage ---
storage_client = storage.Client()

# --- Configuration du logging ---
log_stream = io.StringIO()
logging.basicConfig(
    stream=log_stream,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


def file_exists_in_gcs(bucket_name, gcs_path):
    """Vérifie si un fichier existe déjà dans GCS."""
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(gcs_path)
    return blob.exists()


def upload_log_to_gcs():
    """Upload du fichier de log vers GCS."""
    log_filename = f"{GCS_LOG_FOLDER}extract_log_{datetime.now(UTC).strftime('%Y%m%d_%H%M%S')}.log"
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(log_filename)
    blob.upload_from_string(log_stream.getvalue())
    logging.info(f"Log file uploaded to {log_filename}")


def download_histo_data():
    """
    Télécharge les fichiers PARQUET des taxis jaunes (Yellow Taxi) de 2022 à aujourd'hui
    et les envoie directement sur Google Cloud Storage.
    """
    current_year = datetime.now().year

    try:
        for year in range(2022, current_year + 1):
            for month in range(1, 13):
                file_name = f"yellow_tripdata_{year}-{month:02d}.parquet"
                gcs_path = f"{GCS_FOLDER}{file_name}"
                download_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{file_name}"

                if file_exists_in_gcs(BUCKET_NAME, gcs_path):
                    logging.info(f"{file_name} already exists in GCS, skipping...")
                    continue

                try:
                    logging.info(f"Downloading {file_name}...")
                    response = requests.get(download_url, stream=True)

                    if response.status_code == 200:
                        bucket = storage_client.bucket(BUCKET_NAME)
                        blob = bucket.blob(gcs_path)
                        blob.upload_from_string(response.content)
                        logging.info(f"{file_name} uploaded to GCS at {gcs_path}")
                    elif response.status_code == 404:
                        logging.warning(f"File {file_name} not found on source, skipping...")
                    else:
                        logging.error(f"Failed to download {file_name}. HTTP status code: {response.status_code}")

                except Exception as e:
                    logging.error(f"Error downloading {file_name}: {str(e)}")

                time.sleep(1)  # pause d’une seconde entre chaque téléchargement pour éviter la surcharge

        logging.info("Download and upload to GCS completed successfully!")

    except Exception as e:
        logging.error(f"Unexpected error: {str(e)}")

    finally:
        upload_log_to_gcs()


if __name__ == '__main__':
    logging.info(f"Date of historical data download: {datetime.today()}")
    download_histo_data()
