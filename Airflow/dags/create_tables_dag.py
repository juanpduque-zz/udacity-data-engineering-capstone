from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pandas as pd
from operators.convert_csv import ConvertCsv
from operators.create_dimensions import DimensionTableOperator
from operators.create_tables import CreateTableOperator
from operators.etl import ETLOperator
from operators.data_quality import DataQualityOperator
from operators.ensure_distinct import EnsureDistinctRecords
from operators.helpers.read_dataframes import read_immigration_data, read_demographics, read_airport_codes,read_countries, read_visa_codes, read_travel_mode
from operators.helpers.clean_dfs import clean_demographics, clean_airport_codes, clean_immigration, clean_countries, clean_visa_codes, clean_travel_mode

import logging

logging.basicConfig(level=logging.INFO)

default_args = {
    'owner': 'juanp',
    'start_date': datetime(2019, 6, 24),
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'depends_on_past': False,
}

dag = DAG('immigration_etl_pipeline',
          default_args=default_args,
          description='create tables in redshift.',
          schedule_interval=None,
      )

def begin_execution():
    logging.info("Create Table Execution Starting.")

def finish_execution():
    logging.info("Create Table Execution Completed.")

start_operator = PythonOperator(task_id='begin_execution',
                                python_callable=begin_execution,
                                dag=dag)

convert_sas_to_csv = ConvertCsv(
    task_id='sas_to_csv',
    dag=dag,
    provide_context=True
)

create_immigration = CreateTableOperator(task_id='create_immigration_table',
                                         table_name='immigrations',
                                         dag=dag)                                                                              

create_countries = CreateTableOperator(task_id='create_countries',
                                       table_name='countries',
                                       dag=dag)

create_demographics = CreateTableOperator(task_id='create_demographics',
                                          table_name='demographics',
                                          dag=dag)

create_cities = CreateTableOperator(task_id='create_cities',
                                    table_name='cities',
                                    dag=dag)                                                                                                                        

create_airport_codes = CreateTableOperator(task_id='create_airport_codes',
                                           table_name='airport_codes',
                                           dag=dag)           

create_travel_mode = CreateTableOperator(task_id='create_travel_mode',
                                           table_name='travel_mode',
                                           dag=dag)

create_visa_codes = CreateTableOperator(task_id='create_visa_codes',
                                           table_name='visa_codes',
                                           dag=dag)

etl_immigration_data = ETLOperator(task_id='etl_immigration_data',
                                   read_df=read_immigration_data,
                                   clean_df=clean_immigration,
                                   table_name='immigrations',
                                   write_subset=False,
                                   write_sample=1000,
                                   dag=dag)                                 


etl_demographics = ETLOperator(task_id='etl_demographics_data',
                               read_df=read_demographics,
                               clean_df=clean_demographics,
                               table_name='demographics',
                               write_subset=False,
                               write_sample=1000,
                               dag=dag)
                    
etl_airport_codes = ETLOperator(task_id='etl_airport_codes_data',
                                read_df=read_airport_codes,
                                clean_df=clean_airport_codes,
                                table_name='airport_codes',
                                write_subset=False,
                                write_sample=1000,
                                dag=dag)

create_cities_sql = """
insert into cities (state_code, state, name)
select state_code, state, city from demographics group by state_code, state, city;
"""    

etl_cities = DimensionTableOperator(task_id='etl_cities_data',
                                        sql_statement=create_cities_sql,
                                        dag=dag)

etl_countries = ETLOperator(task_id='etl_countries_data',
                                read_df=read_countries,
                                clean_df=clean_countries,
                                table_name='countries',
                                write_subset=False,
                                write_sample=True,
                                dag=dag)

etl_visa_codes = ETLOperator(task_id='etl_visa_codes_data',
                                read_df=read_visa_codes,
                                clean_df=clean_visa_codes,
                                table_name='visa_codes',
                                write_subset=False,
                                write_sample=False,
                                dag=dag)

etl_travel_mode = ETLOperator(task_id='etl_travel_mode_data',
                                read_df=read_travel_mode,
                                clean_df=clean_travel_mode,
                                table_name='travel_mode',
                                write_subset=False,
                                write_sample=False,
                                dag=dag)


run_quality_checks = DataQualityOperator(
    task_id='run_data_quality_checks',
    redshift_conn_id="Redshift",
    tables=['immigrations', 'countries', 'demographics', 'cities', 'airport_codes','travel_mode','visa_codes'],
    dag=dag
)

check_immigration_distinct_records = EnsureDistinctRecords(task_id='check_distinct_immigration_records',
                                                           table_name='immigrations',
                                                           distinct_column='admission_number',
                                                           dag=dag)

check_cities_distinct_records = EnsureDistinctRecords(task_id='check_distinct_city_records',
                                                      table_name='cities',
                                                      distinct_column='city_id',
                                                      dag=dag)     

check_countries_distinct_records = EnsureDistinctRecords(task_id='check_distinct_country_records',
                                                         table_name='countries',
                                                         distinct_column='country_id',
                                                         dag=dag)

check_airport_codes_distinct_records = EnsureDistinctRecords(task_id='check_airport_codes_distinct_records',
                                                             table_name='airport_codes',
                                                             distinct_column='id_airport',
                                                             dag=dag)           

check_visa_codes_distinct_records = EnsureDistinctRecords(task_id='check_visa_codes_distinct_records',
                                                             table_name='visa_codes',
                                                             distinct_column='visa_id',
                                                             dag=dag) 

check_travel_mode_distinct_records = EnsureDistinctRecords(task_id='check_travel_mode_distinct_records',
                                                             table_name='travel_mode',
                                                             distinct_column='travel_id',
                                                             dag=dag) 


end_operator = PythonOperator(
    task_id='end_execution',
    python_callable=finish_execution,
    dag=dag
)
                                                                                                                                                                                                                                                                                                                                                                                                                                                                        

start_operator >> create_immigration >> etl_immigration_data >> check_immigration_distinct_records >> run_quality_checks
start_operator >> create_demographics >> etl_demographics >> create_cities >> etl_cities >> check_cities_distinct_records >> run_quality_checks
start_operator >> create_airport_codes >> etl_airport_codes >> check_airport_codes_distinct_records >> run_quality_checks
start_operator >> convert_sas_to_csv >> create_countries >> etl_countries >> check_countries_distinct_records >> run_quality_checks
start_operator >> convert_sas_to_csv >> create_travel_mode >>  etl_travel_mode >> check_travel_mode_distinct_records >> run_quality_checks
start_operator >> convert_sas_to_csv >> create_visa_codes >>  etl_visa_codes >> check_visa_codes_distinct_records >> run_quality_checks


run_quality_checks >> end_operator