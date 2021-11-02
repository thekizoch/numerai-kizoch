import time
from google.cloud import bigquery 
from google.cloud import storage
import pandas as pd
import numerapi
from google.cloud import pubsub_v1

# function to upload from local disk to GCS using python API
def upload_blob(bucket_name, source_file_name, destination_blob_name):
  """Uploads a file to the bucket."""
  # The ID of your GCS bucket
  # bucket_name = "your-bucket-name"
  # The path to your file to upload
  # source_file_name = "local/path/to/file"
  # The ID of your GCS object
  # destination_blob_name = "storage-object-name"

  storage_client = storage.Client()
  bucket = storage_client.bucket(bucket_name)
  blob = bucket.blob(destination_blob_name)

  blob.upload_from_filename(source_file_name)

  print(
      "File {} uploaded to {}.".format(
          source_file_name, destination_blob_name
      )
  )

# Publishes a message to a Cloud Pub/Sub topic.
def publish():
    
    publisher = pubsub_v1.PublisherClient()
    
    topic_path = "projects/numerai-kizoch/topics/load-tournament-data-weekly-step-3"
    message_bytes = b'do step 3'
    # Publishes a message
    try:
        publish_future = publisher.publish(topic_path, data=message_bytes)
        publish_future.result()  # Verify the publish succeeded
        return 'Message published.'
    except Exception as e:
        print(e)
        return (e, 500)

def run(event, context):

  # start timer
  start = time.time()

  # instantiate clients
  napi = numerapi.NumerAPI()
  bqclient = bigquery.Client(project='numerai-kizoch')

  current_round = napi.get_current_round()  

  # define paths
  file_path_tournament = f"tmp/numerai_tournament_data_{current_round}.parquet"
  uri_parquet = "gs://numerai-kizoch/" + file_path_tournament



  # load tournament parquet into BQ
  table_id = "numerai-kizoch.big_data.tournament_data"

  job_config = bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.PARQUET, \
                                    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)
  uri = uri_parquet

  load_job = bqclient.load_table_from_uri(
      uri, table_id, job_config=job_config
  )  # Make an API request.

  # doest wait for job to complete 
  #load_job.result()  

  # waits for 500 seconds to elapse
  while (time.time() - start) < 500:
    time.sleep(10)

  # call next cloud function
  publish()

