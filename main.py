# Service account
# 926027561366@cloudbuild.gserviceaccount.com

from google.cloud import bigquery
from flask import Flask, request, jsonify
import os
import json
import logging
from urllib.parse import quote

app = Flask(__name__)

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize BigQuery client
client = bigquery.Client()

@app.route('/')
def main():
    try:
        table_id = "gcp-serverless-rroiectl.usa_states.states"

        logger.info(f"Target table ID: {table_id}")

        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
        )

        # uri = "gs://{your-bucket-name}/us-states/us-states.csv"
        uri = "gs://gcp-serverless-rroiectl-test-bucket/us-states.csv"
        logger.info(f"Source file URI: {uri}")

        load_job = client.load_table_from_uri(
            uri, table_id, job_config=job_config
        )

        logger.info("Starting the load job")
        load_job.result()  # Wait for the job to complete
        logger.info("Load job completed")

        destination_table = client.get_table(table_id)
        logger.info(f"Loaded {destination_table.num_rows} rows into {table_id}")

        return jsonify({"data": destination_table.num_rows})

    except Exception as e:
        logger.error(f"Error during BigQuery operation: {str(e)}")
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 5052)))
