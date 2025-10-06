import logging
from typing import Union

import numpy as np
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

logger = logging.getLogger(__name__)

# NOTE: Set the GOOGLE_APPLICATION_CREDENTIALS environment variable with the path to the JSON auth file.


def convert_datatypes(df: pd.DataFrame, schema: list[bigquery.SchemaField]) -> pd.DataFrame:
    """
    Converts DataFrame columns to appropriate formats based on the provided BigQuery schema.

    Args:
        df (pd.DataFrame): The input DataFrame.
        schema (list[bigquery.SchemaField]): Schema of the target BigQuery table.

    Returns:
        pd.DataFrame: Transformed DataFrame with columns cast to BigQuery-compatible types.
    """
    numeric_types = ['BOOLEAN', 'FLOAT', 'INT', 'NUMERIC', 'INTEGER']
    datetime_types = ['TIMESTAMP', 'DATE', 'DATETIME', 'TIME']

    schema_cols = []
    for col in schema:
        col_name = col.name
        col_type = col.field_type
        col_mode = col.mode
        schema_cols.append(col_name)

        # Create column if it does not exist
        if col_name not in df.columns:
            df[col_name] = None

        # Convert to appropriate type
        if col_type in numeric_types:
            df[col_name] = df[col_name].fillna(np.nan)
            df.loc[df[col_name] == 'None', col_name] = np.nan
            df.loc[df[col_name] == 'False', col_name] = 0
            df.loc[df[col_name] == 'True', col_name] = 1
            df[col_name] = df[col_name].astype(float)
        elif col_type in datetime_types:
            df[col_name] = pd.to_datetime(df[col_name], infer_datetime_format=True, utc=True, errors='coerce')
        elif col_type == 'STRING' and col_mode == 'REPEATED':
            # Leave as-is for repeated fields (BQ ARRAYs)
            pass
        else:
            df[col_name] = df[col_name].astype(str)

    return df[schema_cols]


class BigQuery:
    """
    A simple wrapper for interacting with Google BigQuery using the Python client library.
    """

    def __init__(self, **kwargs):
        """
        Initializes the BigQuery client.

        Args:
            **kwargs: Keyword arguments passed to bigquery.Client.
        """
        self.client = bigquery.Client(**kwargs)

    @classmethod
    def from_json_credentials(cls, json_credentials: dict) -> "BigQuery":
        """
        Instantiates the class using JSON credentials.

        Args:
            json_credentials (dict): Parsed service account JSON credentials.

        Returns:
            BigQuery: An instance of the BigQuery client wrapper.
        """
        credentials = service_account.Credentials.from_service_account_info(json_credentials)
        return cls(credentials=credentials)

    def get_data_from_query(self, sql: str) -> pd.DataFrame:
        """
        Executes a SQL query and returns the result as a DataFrame.

        Args:
            sql (str): SQL query to execute.

        Returns:
            pd.DataFrame: Query results.
        """
        logger.info("Running query and fetching results.")
        return self.client.query(sql).to_dataframe()

    def insert_many(self, df: pd.DataFrame, table_name: str, streaming: bool = False) -> None:
        """
        Inserts data from a DataFrame into a BigQuery table.

        Args:
            df (pd.DataFrame): DataFrame containing the data to insert.
            table_name (str): Full table identifier (project.dataset.table).
            streaming (bool): If True, use streaming inserts; otherwise, use batch load.

        Returns:
            None
        """
        logger.info("Converting data types based on schema.")
        table = self.client.get_table(table_name)
        df_to_db = convert_datatypes(df, table.schema)

        logger.info(f"Loading data into table: {table_name}")
        if streaming:
            errors = self.client.insert_rows_from_dataframe(table, df_to_db)
            errors_flattened = [e for batch in errors for e in batch]
            if not errors_flattened:
                logger.info(f"Inserted {df_to_db.shape[0]} rows via streaming to {table_name}.")
            else:
                logger.warning(f"Errors encountered during streaming insert: {errors_flattened}")
        else:
            job = self.client.load_table_from_dataframe(df_to_db, table)
            result = job.result()
            logger.info(f"Loaded {result.output_rows} rows to {result.destination} via batch load.")

    def execute_query(self, query: str) -> bigquery.job.QueryJob:
        """
        Executes a SQL query without waiting for results.

        Args:
            query (str): SQL query to execute.

        Returns:
            bigquery.job.QueryJob: The QueryJob object.
        """
        logger.info(f"Executing query: {' '.join(query.split())}")
        job = self.client.query(query)
        logger.info("Query execution started.")
        return job

    def create_table(self, table: str, schema: list, drop_first: bool = False) -> None:
        """
        Creates a BigQuery table with the specified schema.

        Args:
            table (str): Full table name (project.dataset.table).
            schema (list): List of bigquery.SchemaField instances.
            drop_first (bool): If True, drop the table if it exists before creating.

        Returns:
            None
        """
        if drop_first:
            logger.info(f"Dropping existing table: {table}")
            self.client.delete_table(table, not_found_ok=True)

        logger.info(f"Creating table: {table}")
        bq_table = bigquery.Table(table, schema=schema)
        self.client.create_dataset(dataset=bq_table.dataset_id, exists_ok=True)
        result = self.client.create_table(bq_table, exists_ok=True)

        logger.info(f"Table created: {result.project}.{result.dataset_id}.{result.table_id}")