from google.cloud import bigquery
from google.cloud import storage
import pandas as pd
import numerapi

import subprocess
import os

def download_blob(bucket_name, source_blob_name, destination_file_name):
    """Downloads a blob from the bucket."""
    # The ID of your GCS bucket
    # bucket_name = "your-bucket-name"

    # The ID of your GCS object
    # source_blob_name = "storage-object-name"

    # The path to which the file should be downloaded
    # destination_file_name = "local/path/to/file"

    storage_client = storage.Client()

    bucket = storage_client.bucket(bucket_name)

    # Construct a client side representation of a blob.
    # Note `Bucket.blob` differs from `Bucket.get_blob` as it doesn't retrieve
    # any content from Google Cloud Storage. As we don't need additional data,
    # using `Bucket.blob` is preferred here.
    blob = bucket.blob(source_blob_name)
    blob.download_to_filename(destination_file_name)

    print(
        "Downloaded storage object {} from bucket {} to local file {}.".format(
            source_blob_name, bucket_name, destination_file_name
        )
    )

def submit_bq_submissions():
  # define environment variables
  PROJECT = 'numerai-kizoch'
  CURRENT_ROUND = numerapi.NumerAPI().get_current_round()

  # define MODEL_NAME's and MODEL_ID's
  bqclient = bigquery.Client()

  # create df of all bq models
  df_model_registry = bqclient.query('SELECT * FROM `model_registry`.__TABLES__;').to_dataframe()
  list_table_ids = list(df_model_registry.table_id)
  df_bq_models = pd.DataFrame()

  for table in list_table_ids:
    df_temp = bqclient.query('SELECT * FROM model_registry.{}'.format(table)).to_dataframe()
    df_bq_models = pd.concat([df_bq_models,df_temp])

  # write all submission.csv's to GCS, and download to local /tmp
  for index, row in df_bq_models.iterrows():
    
    MODEL_NAME = row[0]

    # write to GCS
    destination_uri = "gs://{}/submissions/round_{}/submission_{}.csv".format(
      PROJECT, CURRENT_ROUND, MODEL_NAME, )
    results_table_id = "numerai-kizoch.predictions.{}".format(MODEL_NAME)
    extract_job = bqclient.extract_table(
      results_table_id,
      destination_uri,
      location="US")  # API request
    extract_job.result()  # Waits for job to complete

    # download
    source_blob_name = "submissions/round_{}/submission_{}.csv".format(
      CURRENT_ROUND, MODEL_NAME)
    destination_file_name = '/tmp/submission_{}.csv'.format(MODEL_NAME)

    download_blob(bucket_name=PROJECT, \
                  source_blob_name=source_blob_name, \
                  destination_file_name=destination_file_name)

  # authentication
  public_id = "HSOL3P7JYNV3D7IXEU56H26NXN7JETDO"
  secret_key = os.getenv('SECRET_KEY')
  napi = numerapi.NumerAPI(public_id=public_id, secret_key=secret_key)

  for index, row in df_bq_models.iterrows():

    MODEL_NAME = row[0]
    MODEL_LOCATION = row[1]
    MODEL_ID = row[2]

    destination_uri = "gs://{}/submissions/round_{}/submission_{}.csv".format(
      PROJECT, CURRENT_ROUND, MODEL_NAME, )
    
    # local path
    file_path_submission = "/tmp/submission_{}.csv".format(MODEL_NAME)

    if MODEL_LOCATION.find('v1_models') == -1:
      model_version = 2
    else:
      model_version = 1

    # submit
    submission_id = napi.upload_predictions(file_path_submission, \
                            model_id=MODEL_ID, \
                            version=model_version)
    
    print("The submission_id is {} for model {}".format(submission_id, \
                                                        MODEL_NAME))

def run(context, event):
  submit_bq_submissions()

