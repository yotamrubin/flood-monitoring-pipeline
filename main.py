import requests
import pandas as pd
import functions_framework
from google.cloud import bigquery

API_URL = "https://environment.data.gov.uk/flood-monitoring/id/floods"

def fetch_flood_data():
    """Fetch real-time flood data from API"""
    response = requests.get(API_URL)
    data = response.json().get("items", [])
    return data

def rename_id(df):
    """Rename '@id' column to 'id'"""
    if '@id' in df.columns:
        df.rename(columns={'@id': 'id'}, inplace=True)
    return df


def transform_data(data):
    """Convert API response to DataFrame and clean column names"""
    df = pd.DataFrame(data)
    df = rename_id(df)  # Rename '@id' to 'id'

    # Keep all columns except 'eaRegionName'
    df = df[['id', 'description', 'eaAreaName', 'floodArea', 'floodAreaID', 
             'isTidal', 'message', 'severity', 'severityLevel', 
             'timeMessageChanged', 'timeRaised', 'timeSeverityChanged']]
    return df


def load_to_bigquery(df):
    """Load flood data to BigQuery"""
    client = bigquery.Client()
    table_id = "your_project.flood_data.flood_reports"

    job = client.load_table_from_dataframe(df, table_id)
    job.result()  # Wait for completion

@functions_framework.http
def main(request):
    """Cloud Function Entry Point (Matches Entry Field)"""
    raw_data = fetch_flood_data()
    df = transform_data(raw_data)
    load_to_bigquery(df)
    return "Flood data loaded to BigQuery"
