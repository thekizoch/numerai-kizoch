from google.cloud import bigquery
import numerapi

# Predict on all v1 BQ models func
def predict_v1_bq():
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

    query_job = bqclient.query(sql, job_config=job_config)


def run(context, event):
  predict_v1_bq()
