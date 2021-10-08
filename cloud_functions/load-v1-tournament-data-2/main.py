from google.cloud import bigquery 
import pandas as pd
import numerapi
from google.cloud import pubsub_v1

# Publishes a message to a Cloud Pub/Sub topic.
def publish():
    
    publisher = pubsub_v1.PublisherClient()
    
    topic_path = "projects/numerai-kizoch/topics/load-v1-tournament-data-weekly-step-3"
    message_bytes = b'do step 3'
    # Publishes a message
    try:
        publish_future = publisher.publish(topic_path, data=message_bytes)
        publish_future.result()  # Verify the publish succeeded
        return 'Message published.'
    except Exception as e:
        print(e)
        return (e, 500)

def run(context, event):
    
  # instantiate clients
  napi = numerapi.NumerAPI()
  bqclient = bigquery.Client(project='numerai-kizoch')

  current_round = napi.get_current_round()  

  # define paths
  file_path_tournament = f"tmp/numerai_v1_tournament_data_{current_round}.parquet"
  uri_parquet = "gs://numerai-kizoch/" + file_path_tournament


  # load tournament parquet into BQ
  table_id = "numerai-kizoch.data.tournament_data"

  job_config = bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.PARQUET, \
                                      write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)
  uri = uri_parquet

  load_job = bqclient.load_table_from_uri(
      uri, table_id, job_config=job_config
  )  # Make an API request.

  # doest wait for job to complete 
  load_job.result()  

  # call next cloud function
  publish()

