import http.client
import json
import csv
import io
from datetime import datetime
from dateutil.relativedelta import relativedelta  # Import the package for relative date manipulation
from google.cloud import storage

# Environment variables
BUCKET_NAME = 'us-central1-dataengineering-253045e9-bucket'
CSV_FILENAME = "weather_data.csv"

def main(event=None, context=None):
    # Initialize variables
    all_weather_data = []
    last_date = datetime.now().strftime("%Y-%m-%d")  # Get today's date in YYYY-MM-DD format
    date = (datetime.now() - relativedelta(months=12)).strftime("%Y-%m-%d")  # Date 6 months before the last_date
    lat = 52.52
    lon = 9.9
    page = 1
    max_pages = 4  # Set the page limit

    # Fetch and save weather data
    while page <= max_pages:
        print(f"Fetching data from page {page}...")
        weather_data_page = fetch_weather_data(date, last_date, lat, lon, page)
        if 'weather' in weather_data_page and weather_data_page['weather']:
            all_weather_data.extend(weather_data_page['weather'])
            page += 1
        else:
            print(f"No more data found on page {page}. Stopping.")
            break

    print(f"Total records fetched: {len(all_weather_data)}")

    # Convert to CSV and save locally
    csv_data = json_to_csv(all_weather_data)
    upload_to_gcs(BUCKET_NAME, CSV_FILENAME, csv_data)

    return 'Completed successfully'

def fetch_weather_data(date, last_date, lat, lon, page):
    try:
        conn = http.client.HTTPSConnection("api.brightsky.dev")
        headers = {'Accept': "application/json"}
        request_path = f"/weather?date={date}&last_date={last_date}&lat={lat}&lon={lon}&page={page}"
        conn.request("GET", request_path, headers=headers)
        res = conn.getresponse()
        data = res.read()
        return json.loads(data.decode("utf-8"))
    except Exception as e:
        print(f"Failed to fetch data due to: {e}")
        return {}  # Return an empty dict on failure

def json_to_csv(json_data):
    if not json: 
        return ""
    output = io.StringIO()
    writer = csv.writer(output)
    if json_data:
        headers = json_data[0].keys()
        writer.writerow(headers)
        for entry in json_data:
            writer.writerow(entry.values())
    return output.getvalue()

def upload_to_gcs(bucket_name, csv_filename, csv_data):
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(csv_filename)
        blob.upload_from_string(csv_data)
        print(f"CSV data uploaded to {csv_filename} in bucket {bucket_name}.")
    except Exception as e:
        print(f"Failed to upload data to GCS: {e}")

if __name__ == "__main__":
    main()
