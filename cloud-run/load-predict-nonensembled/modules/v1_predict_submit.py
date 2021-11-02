from google.cloud import bigquery
from google.cloud import storage
import os
import numerapi


# Predict on all v1 BQ models func
def predict_bq_v1():
    # df from bq of model registry
    bqclient = bigquery.Client()
    df_v1_bq_models = bqclient.query("SELECT * FROM \
    `numerai-kizoch.model_registry.v1_bq_models`").to_dataframe()

    # Loop on all v1 BQ models
    for index, row in df_v1_bq_models.iterrows():
        # define model from bq table
        MODEL_NAME = row[0]
        MODEL_LOCATION = row[1]

        # Set table id to the ID of the destination table.
        results_table_id = "numerai-kizoch.predictions.{}".format(MODEL_NAME)

        # config set to overwrite previous predictions
        job_config = bigquery.QueryJobConfig(destination=results_table_id, \
                                             write_disposition='WRITE_TRUNCATE')

        sql = """
    SELECT
      id, predicted_target AS prediction
    FROM
      ML.PREDICT (MODEL `{}`,
        (
        SELECT
          * EXCEPT (era, data_type)
        FROM
          `numerai-kizoch.data.tournament_data`
        )
      )
    ORDER BY row ASC
      """.format(MODEL_LOCATION)

        # execute and wait for job to complete
        query_job = bqclient.query(sql, job_config=job_config)
        query_job.result()


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


def submit_bq_submissions_v1():
    # define environment variables
    PROJECT = 'numerai-kizoch'
    CURRENT_ROUND = numerapi.NumerAPI().get_current_round()
    model_version = 1

    # authenticate
    public_id = "HSOL3P7JYNV3D7IXEU56H26NXN7JETDO"
    secret_key = os.getenv('SECRET_KEY')
    napi = numerapi.NumerAPI(public_id=public_id, secret_key=secret_key)

    # define MODEL_NAME's and MODEL_ID's
    bqclient = bigquery.Client()

    # create df of all bq models
    df_model_registry = bqclient.query('SELECT * FROM `model_registry`.__TABLES__;').to_dataframe()
    list_table_ids = list(df_model_registry.table_id)

    for table_id in list_table_ids:
        if 'v1_bq_models' == table_id: # finds matching bq table
            df_bq_v1_models = bqclient.query('SELECT * FROM model_registry.{}'.format(table_id)).to_dataframe()

    # write all submission.csv's to GCS, and download to local /tmp
    for index, row in df_bq_v1_models.iterrows():
        MODEL_NAME = row[0]

        # write to GCS
        destination_uri = "gs://{}/tmp/submissions/round_{}/submission_{}.csv".format(
            PROJECT, CURRENT_ROUND, MODEL_NAME, )
        results_table_id = "numerai-kizoch.predictions.{}".format(MODEL_NAME)
        extract_job = bqclient.extract_table(
            results_table_id,
            destination_uri,
            location="US")  # API request
        extract_job.result()  # Waits for job to complete

        # download
        source_blob_name = "tmp/submissions/round_{}/submission_{}.csv".format(
            CURRENT_ROUND, MODEL_NAME)
        destination_file_name = '/tmp/submission_{}.csv'.format(MODEL_NAME)

        download_blob(bucket_name=PROJECT, \
                      source_blob_name=source_blob_name, \
                      destination_file_name=destination_file_name)

    # submit and delete file to recover memory
    for index, row in df_bq_v1_models.iterrows():
        MODEL_NAME = row[0]
        MODEL_ID = row[2]

        # local path
        file_path_submission = "/tmp/submission_{}.csv".format(MODEL_NAME)

        # submit
        submission_id = napi.upload_predictions(file_path_submission, \
                                                model_id=MODEL_ID, \
                                                version=model_version)

        print("The submission_id is {} for model {}".format(submission_id, \
                                                            MODEL_NAME))
        # remove local submission file to recover memory
        os.remove(file_path_submission)
