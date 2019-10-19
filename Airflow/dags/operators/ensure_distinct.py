from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

import logging

from operators.helpers.create_table_statements import drop_table_if_exists, create_statements

class EnsureDistinctRecords(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 table_name,
                 distinct_column,
                 redshift_conn_id='Redshift',
                 *args, **kwargs):

        super(EnsureDistinctRecords, self).__init__(*args, **kwargs)
        self.table_name = table_name
        self.distinct_column = distinct_column
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        # self.log.info('LoadDimensionOperator not implemented yet')
        try:
            conn = PostgresHook(self.redshift_conn_id).get_conn()

            logging.info(f"Ensuring Distinct Records::{self.table_name}, on::{self.distinct_column}")

            cursor = conn.cursor()

            cursor.execute(f'select count(*) from {self.table_name}')

            total_count = cursor.fetchone()

            logging.info(f"Total Count::{total_count}")

            cursor.execute(f'select count (distinct {self.distinct_column}) from {self.table_name}')

            unique_count = cursor.fetchone()

            logging.info(f"Unique Count::{unique_count}")

            if total_count != unique_count:
                raise Exception("Total count does not match unique count. Invalid duplicate records.")

            conn.commit()

            cursor.close()

            conn.close()

        except Exception as e:
            raise e
