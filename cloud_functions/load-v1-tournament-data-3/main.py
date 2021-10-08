from google.cloud import bigquery 
import numerapi
from google.cloud import pubsub_v1

# Publishes a message to a Cloud Pub/Sub topic.
def publish():
    
    publisher = pubsub_v1.PublisherClient()
    
    topic_path = "projects/numerai-kizoch/topics/v1-weekly-prediction"
    message_bytes = b'start weekly prediction'
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
  file_path_mapping = "tmp/v1_mapping_round_{}.csv".format(current_round)
  uri_csv = "gs://numerai-kizoch/" + file_path_mapping

  # add new column 'row'
  table_id = "numerai-kizoch.data.tournament_data"

  table = bqclient.get_table(table_id)  # Make an API request.

  original_schema = table.schema
  new_schema = original_schema[:]  # Creates a copy of the schema.
  new_schema.append(bigquery.SchemaField("row", "NUMERIC"))

  table.schema = new_schema
  table = bqclient.update_table(table, ["schema"])  # Make an API request.

  if len(table.schema) == len(original_schema) + 1 == len(new_schema):
      print("A new column has been added.")
  else:
      print("The column has not been added.")

  # create temp table with id, row
  table_id = "data.temp"

  job_config = bigquery.LoadJobConfig(
      schema=[
          bigquery.SchemaField("id", "STRING"),
          bigquery.SchemaField("row", "NUMERIC"),
      ],
      skip_leading_rows=1,
      # The source format defaults to CSV, so the line below is optional.
      source_format=bigquery.SourceFormat.CSV,
  )

  uri = uri_csv

  load_job = bqclient.load_table_from_uri(
      uri, table_id, job_config=job_config)  # Make an API request.

  load_job.result()  # Waits for the job to complete.

  destination_table = bqclient.get_table(table_id)  # Make an API request.
  print("Loaded {} rows in table {}".format(destination_table.num_rows, \
                                            table_id))

  # join temp table into tournament table
  sql = """
      UPDATE data.tournament_data d
      SET row = t.row
      FROM data.temp t
      WHERE d.id = t.id
        """
  query_job = bqclient.query(sql)
  query_job.result() # waits for job to finish

  # delete temp table
  table_id = "numerai-kizoch.data.temp"
  bqclient.delete_table(table_id, not_found_ok=True)  # Make an API request.
  print("Deleted table '{}'.".format(table_id))

  # call next cloud function
  publish()

