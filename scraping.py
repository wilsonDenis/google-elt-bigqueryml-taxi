import os
import requests
import time
from datetime import datetime
from google.cloud import storage
import logging
import io
from datetime import UTC  # Import UTC explicitly   
LOCAL_FOLDER="ny_taxi_data"
if not os.path.exists(LOCAL_FOLDER):
    os.makedirs(LOCAL_FOLDER)
    
current_year = datetime.now().year
for year in range(2024, current_year + 1):
    for month in range(1, 13):
        file_name = f"yellow_tripdata_{year}-{month:02d}.parquet"
        file_path = os.path.join(LOCAL_FOLDER, file_name)
        downald_url=f"https://d37ci6vzurychx.cloudfront.net/trip-data/{file_name}"
        print(f"Downloading {file_name}...")
        response = requests.get(downald_url, stream=True)
        