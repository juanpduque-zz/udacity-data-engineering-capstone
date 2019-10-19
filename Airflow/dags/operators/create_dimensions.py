from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging 

class DimensionTableOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 sql_statement,
                 redshift_conn_id='Redshift',
                 *args, **kwargs):
        """
        parameters:
        sql_statement: sql statement to execute to create the dimension table inserts
        """

        super(DimensionTableOperator, self).__init__(*args, **kwargs)
        self.sql_statement = sql_statement
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        try:
            logging.info("Dimension Operator::Creating Dimension Tables")
            conn = PostgresHook(self.redshift_conn_id).get_conn()
            logging.info(f"Dimension Operator::Executing Dimension Query::{self.sql_statement}")
            cursor = conn.cursor()
            cursor.execute(self.sql_statement)
            conn.commit()
            cursor.close()
            conn.close()
        except Exception as e:
            raise e