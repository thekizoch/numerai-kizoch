Configuration:
- Python 3.9
- 2 GB of Memory (parquet download is currently ~0.5 G
- Timeout: 540 seconds 

Cloud Function downloads current tournament parquet from numerai and does the following:
- uploads parquet to GCS
- uploads that parquet to BigQuery
- creates CSV with mapping of numbered rows and ids
- create a temp BigQuery table from CSV to update tournament table with numbered rows
