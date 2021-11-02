from google.cloud import bigquery 
from google.cloud import storage
import pandas as pd
import numerapi
from google.cloud import pubsub_v1
import pyarrow

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

    topic_path = "projects/numerai-kizoch/topics/load-tournament-data-weekly-step-2"
    message_bytes = b'do step 2'
    # Publishes a message
    try:
        publish_future = publisher.publish(topic_path, data=message_bytes)
        publish_future.result()  # Verify the publish succeeded
        return 'Message published.'
    except Exception as e:
        print(e)
        return (e, 500)

def run(event, context):
  # delete current set v2 from BQ

  # instantiate clients
  napi = numerapi.NumerAPI()
  bqclient = bigquery.Client(project='numerai-kizoch')
  publisher = pubsub_v1.PublisherClient()

  table_id = "numerai-kizoch.big_data.tournament_data"
  bqclient.delete_table(table_id, not_found_ok=True)  # Make an API request.
  print("Deleted table '{}'.".format(table_id))

  # download current set v2 
  current_round = napi.get_current_round()  

  file_path_tournament = f"tmp/numerai_tournament_data_{current_round}.parquet"
  uri_parquet = "gs://numerai-kizoch/" + file_path_tournament

  napi.download_dataset("numerai_tournament_data.parquet", \
                        "/" + file_path_tournament)
  # upload to GCS
  upload_blob("numerai-kizoch", \
              "/" + file_path_tournament, file_path_tournament)

  # create maping dataframe
  df_map = pd.read_parquet("/" + file_path_tournament, columns=['era'])
  df_map.drop(columns='era', inplace=True)
  df_map['row'] = range(len(df_map))

  # upload csv mapping in GCS
  file_path_mapping = "tmp/mapping_round_{}.csv".format(current_round)
  uri_csv = "gs://numerai-kizoch/" + file_path_mapping
  df_map.to_csv("/" + file_path_mapping)
  upload_blob('numerai-kizoch', "/" + file_path_mapping, file_path_mapping)

  # call other cloud function
  publish()

