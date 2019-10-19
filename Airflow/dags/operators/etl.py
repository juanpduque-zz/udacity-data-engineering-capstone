from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator, Variable
from airflow.utils.decorators import apply_defaults
import pymysql
from sqlalchemy import create_engine
import logging 

class ETLOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 table_name,
                 read_df,
                 clean_df,
                 write_subset=False,
                 write_sample=False,
                 redshift_conn_id='Redshift',
                 redshift_connection_var='amazon-redshift',
                 *args, **kwargs):
        """
        parameters:
        read_df: function to read in the data frame.
        table_name: table to insert the data into.
        """

        super(ETLOperator, self).__init__(*args, **kwargs)
        self.table_name = table_name
        self.read_df = read_df
        self.clean_df = clean_df
        self.write_subset= write_subset
        self.write_sample = write_sample
        self.redshift_conn_id = redshift_conn_id
        self.redshift_connection_var = redshift_connection_var

    def execute(self, context):
        try:
            logging.info("ETLOperator::Reading DF")
            df = self.read_df()
            logging.info("ETL Operator::Read DF.")
        
            df = self.clean_df(df)
            logging.info("ETL Operator::Cleaned DF.")
            logging.info(df.columns)
            logging.info(df.head())

            postgres_url = Variable.get(self.redshift_connection_var)

            logging.info(f"ETL Operator::Connecting to postgres DB on::{postgres_url}")

            engine = create_engine(postgres_url)

            if self.write_subset:
                df = df.iloc[0:10]

            if self.write_sample:
                 df = df.sample(n=self.write_sample)

            print(f'Writing::{len(df)} rows.')

            df.to_sql(self.table_name, engine, if_exists='append', index=False)

            logging.info("ETL Operator::Wrote DataFrame to SQL.")

        except Exception as e:
            raise e
