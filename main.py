import pandas as pd
import urllib.parse
from datetime import datetime, timedelta
from google.cloud import bigquery


def cookie_consent_checker(request):
  table_id = "{{here-your-project-id}}.cookie_consent_check.cookie_consent_request_table"
  if request.url:
    client = bigquery.Client()

    # decode url from gibberish to readable gibberish
    decode_url = urllib.parse.unquote(request.url)
    timestamp_now = datetime.now() + timedelta(hours=2)

    df = pd.DataFrame({
      "timestamp": [timestamp_now],
      "URI": [decode_url]
    })
    
    schema = [
      bigquery.SchemaField("timestamp", "TIMESTAMP"), 
      bigquery.SchemaField("URI", "STRING")
    ]

    try:  
      table = bigquery.Table(table_id, schema)
      table = client.create_table(table)
      job = client.insert_rows_from_dataframe(table=table_id, dataframe=df, selected_fields=schema)
    except Exception:
      print(f"Table '{table_id}' already exists. New row(s) will be inserted to the existing table instead.")
      job = client.insert_rows_from_dataframe(table=table_id, dataframe=df, selected_fields=schema)
    else:
      print(f"Table created: '{table_id}'")
    finally:
      print("Execution completed.")