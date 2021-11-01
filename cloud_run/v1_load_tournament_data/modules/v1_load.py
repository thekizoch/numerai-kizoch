from google.cloud import bigquery
from google.cloud import storage
import logging
import pandas as pd
import os
import shutil
import numerapi


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
        "File {} uploaded to {} in GCS.".format(
            source_file_name, destination_blob_name
        )
    )


def download_v1():
    # delete current v1 data from BQ
    napi = numerapi.NumerAPI()
    bqclient = bigquery.Client(project='numerai-kizoch')
    print('instantiated numerai client')

    table_id = "numerai-kizoch.data.tournament_data"
    bqclient.delete_table(table_id, not_found_ok=True)  # Make an API request.

    print("Deleted table '{}'.".format(table_id))
    logging.info("Deleted table '{}'.".format(table_id))

    # download tournament data v1
    current_round = napi.get_current_round()
    dir_path_tournament = f"tmp/numerai_v1_tournament_data_{current_round}_dir/"
    napi.download_latest_data('tournament', 'parquet', "/" + dir_path_tournament)

    # define the file path in dir
    file_path_tournament = dir_path_tournament + 'latest_numerai_tournament_data.parquet'

    # upload to GCS
    upload_blob("numerai-kizoch", "/" + file_path_tournament, f"tmp/numerai_v1_tournament_data_{current_round}.parquet")

    # create mapping dataframe
    df_map = pd.read_parquet("/" + file_path_tournament, columns=['id'])
    df_map['row'] = range(len(df_map))

    # upload csv mapping in GCS
    file_path_mapping = "tmp/v1_mapping_round_{}.csv".format(current_round)
    uri_csv = "gs://numerai-kizoch/" + file_path_mapping
    df_map.to_csv("/" + file_path_mapping, index=False)
    upload_blob('numerai-kizoch', "/" + file_path_mapping, file_path_mapping)


def load_into_bq_v1():
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

    # wait for job to complete
    load_job.result()

    # deletes dir to recover memory
    shutil.rmtree(f"/tmp/numerai_v1_tournament_data_{current_round}_dir")
    print(f'deleted dir for tournament data')



def add_mapping_v1():
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
    query_job.result()  # waits for job to finish

    # delete temp table
    table_id = "numerai-kizoch.data.temp"
    bqclient.delete_table(table_id, not_found_ok=True)  # Make an API request.
    print("Deleted table '{}'.".format(table_id))

    # deletes file to recover memory
    os.remove('/' + file_path_mapping)
    print(f'deleted file {file_path_mapping}')




