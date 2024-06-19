from io import StringIO
import pandas as pd
from google.cloud import storage,bigquery

def read_data_from_gcs(bucket_name, file_name):
    """Read data from a GCS bucket and return a DataFrame."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    data = blob.download_as_text()
    df = pd.read_csv(StringIO(data))
    print(f"Data read from {bucket_name}/{file_name}")
    return df

def preprocess_data(df):
    """Preprocess the data: Remove duplicates, handle missing values, and convert data types."""
    file_path = "gs://us-central1-dataengineering-253045e9-bucket/weather_data.csv"  # Replace with your file path
    df = pd.read_csv(file_path)
    data = df.drop_duplicates()
    # Handle missing values
    for column in df.columns:
        if data[column].dtype == 'object':
            data[column] = df[column].fillna(df[column].mode()[0])
        else:
            data[column] = df[column].fillna(df[column].mean())

    # Convert data types
    data = data.drop(columns=['precipitation_probability', 'precipitation_probability_6h','fallback_source_ids'])
    data['timestamp'] = pd.to_datetime(df['timestamp'])
    
    # Optionally drop columns if needed (not shown here)
    # df = df.drop(columns=['unnecessary_column'])

    return data

def upload_to_bigquery(data, table_id):
    """Upload the cleaned DataFrame to a BigQuery table."""
    bigquery_client = bigquery.Client()
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        autodetect=True
    )
    job = bigquery_client.load_table_from_dataframe(data, table_id, job_config=job_config)
    job.result()  # Wait for the job to complete
    print(f"Data uploaded to BigQuery table {table_id}")

def main():
    """Main function to process and upload weather data."""
    bucket_name = 'us-central1-dataengineering-253045e9-bucket'
    file_name = 'weather_data.csv'
    table_id = 'data-engineering-2-424618.weather_hourly_alerts.weather_data'

    df = read_data_from_gcs(bucket_name, file_name)
    df_clean = preprocess_data(df)
    upload_to_bigquery(df_clean, table_id)

if __name__ == '__main__':
    main()
