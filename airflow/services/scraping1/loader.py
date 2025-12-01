from google.oauth2 import service_account
from google.cloud import bigquery
from utils.logger import get_logger


logger = get_logger()

def load_local_csv_to_bq(
    csv_path: str,
    dataset_id: str,
    table_id: str,
    project_id: str,
    credential_path: str,
    write_disposition: str = "WRITE_APPEND"
):
    
    credentials = service_account.Credentials.from_service_account_file(credential_path)
    client = bigquery.Client(credentials=credentials, project=project_id)

    table_ref = client.dataset(dataset_id).table(table_id)

    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("totalFunding", "BIGNUMERIC"),
            bigquery.SchemaField("averageLastMonths", "BIGNUMERIC"),
            bigquery.SchemaField("totalBorrowersUsers", "INT64"),
            bigquery.SchemaField("activeUserCnt", "INT64"),
            bigquery.SchemaField("unwithdrawAmt", "BIGNUMERIC"),
            bigquery.SchemaField("tkbRate", "FLOAT64"),
            bigquery.SchemaField("tkb0", "FLOAT64"),
            bigquery.SchemaField("tkb30", "FLOAT64"),
            bigquery.SchemaField("tkb60", "FLOAT64"),
            bigquery.SchemaField("lancar", "FLOAT64"),
            bigquery.SchemaField("userRegisteredOfTheYear", "INT64"),
            bigquery.SchemaField("investmentUsersOfAll", "INT64"),
            bigquery.SchemaField("investmentUsersOfYear", "INT64"),
            bigquery.SchemaField("investingUsers", "INT64"),
            bigquery.SchemaField("borrowUsersOfTheYear", "INT64"),
            bigquery.SchemaField("scraped_at", "TIMESTAMP"),
        ],
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        write_disposition=write_disposition
    )

    with open(csv_path, "rb") as source_file:
        job = client.load_table_from_file(source_file, table_ref, job_config=job_config)

    job.result()  # wait for job to finish
    logger.info(f"Loaded {job.output_rows} rows into {dataset_id}.{table_id}")



def load_df_to_bq(
    df,
    dataset_id: str,
    table_id: str,
    project_id: str,
    credential_path: str,
    write_disposition: str = "WRITE_APPEND"
):
    """
    Load a Pandas DataFrame directly into BigQuery.
    """
    # Create credentials
    credentials = service_account.Credentials.from_service_account_file(credential_path)

    # Init client
    client = bigquery.Client(project=project_id, credentials=credentials)

    # Define table reference
    table_ref = f"{project_id}.{dataset_id}.{table_id}"

    # Define load job config
    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("totalFunding", "BIGNUMERIC"),
            bigquery.SchemaField("averageLastMonths", "BIGNUMERIC"),
            bigquery.SchemaField("totalBorrowersUsers", "INT64"),
            bigquery.SchemaField("activeUserCnt", "INT64"),
            bigquery.SchemaField("unwithdrawAmt", "BIGNUMERIC"),
            bigquery.SchemaField("tkbRate", "FLOAT64"),
            bigquery.SchemaField("tkb0", "FLOAT64"),
            bigquery.SchemaField("tkb30", "FLOAT64"),
            bigquery.SchemaField("tkb60", "FLOAT64"),
            bigquery.SchemaField("lancar", "FLOAT64"),
            bigquery.SchemaField("userRegisteredOfTheYear", "INT64"),
            bigquery.SchemaField("investmentUsersOfAll", "INT64"),
            bigquery.SchemaField("investmentUsersOfYear", "INT64"),
            bigquery.SchemaField("investingUsers", "INT64"),
            bigquery.SchemaField("borrowUsersOfTheYear", "INT64"),
            bigquery.SchemaField("scraped_at", "TIMESTAMP"),
        ],
        write_disposition=write_disposition
    )

    # Load DataFrame into BigQuery
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()  # Wait until finished

    logger.info(f"✅ DataFrame uploaded to {table_ref}, rows: {len(df)}")

