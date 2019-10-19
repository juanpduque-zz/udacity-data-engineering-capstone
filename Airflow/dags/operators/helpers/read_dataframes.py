import os 
import pandas as pd
import logging
import datetime
from pyspark.sql import SparkSession, SQLContext, GroupedData
from pyspark.sql.functions import *
from pyspark.sql.functions import col, split, udf, date_add, current_date
from pyspark.sql.types import DoubleType, IntegerType

def read_immigration_data():

    spark = SparkSession.builder.\
    config("spark.jars.packages","saurfang:spark-sas7bdat:2.0.0-s_2.11")\
    .enableHiveSupport().getOrCreate()

    df_spark=spark.read.parquet("/Users/juanduque/airflow/data/immigration-data")

    get_date = udf(lambda x: datetime.datetime(1,1,1).strftime('%Y-%m-%d %H:%M:%S') if x is None else\
               (datetime.datetime(1960, 1, 1) + datetime.timedelta(seconds=int(x*24*60*60)))\
               .strftime('%Y-%m-%d %H:%M:%S'))

    immigration_data = df_spark.withColumn("cicid", df_spark.cicid.cast(IntegerType()))\
                               .withColumn("i94yr", df_spark.i94yr.cast(IntegerType()))\
                               .withColumn("i94mon", df_spark.i94mon.cast(IntegerType()))\
                               .withColumn("i94cit", df_spark.i94cit.cast(IntegerType()))\
                               .withColumn("i94res", df_spark.i94res.cast(IntegerType()))\
                               .withColumn("arrival_date", get_date(df_spark.arrdate))\
                               .withColumn("mode", df_spark.i94mode.cast(IntegerType()))\
                               .withColumn("mode_text", when(df_spark.i94mode==1, "Air")\
                                           .when(df_spark.i94mode==2, "Sea")\
                                           .when(df_spark.i94mode==3, "Land")\
                                           .otherwise("Not Reported"))\
                               .withColumn("departure_date", get_date(df_spark.depdate))\
                               .withColumn("i94bir", df_spark.i94bir.cast(IntegerType()))\
                               .withColumn("visa", df_spark.i94visa.cast(IntegerType()))\
                               .withColumn("visa_text", when(df_spark.i94visa==1, "Business")\
                                           .when(df_spark.i94visa==2, "Pleasure")\
                                           .when(df_spark.i94visa==3, "Student")\
                                           .otherwise("Not Reported"))\
                               .withColumn("bityear", df_spark.biryear.cast(IntegerType()))\
                               .withColumn("admnum", df_spark.admnum.cast(IntegerType()))\
                               .selectExpr("cicid", "i94yr", "i94mon", \
                   "i94cit", "i94res", "i94port", "arrival_date", \
                   "mode", "mode_text", "i94addr", "departure_date", "i94bir", "visa", "visa_text", "matflag", \
                   "biryear", "gender", "airline", "admnum", "fltno", "visatype")\

    logging.info(f"Read Parquet Files::{immigration_data.columns}")
    logging.info(f"Read all Parquet Files::{immigration_data.head()}")

    return immigration_data


def read_demographics():
  

    data_path = "/Users/juanduque/airflow/data/us-cities-demographics.csv"

    demographics = pd.read_csv(data_path, delimiter=';', index_col=None)

    logging.info(f'Read Demographics::{demographics.head()}')
    logging.info(f'Demographics Columns::{demographics.columns}')

    return demographics

def read_airport_codes():
   

    data_path = "/Users/juanduque/airflow/data/airport-codes_csv.csv"

    airport_codes = pd.read_csv(data_path)

    logging.info(f'Read Airport Codes::{airport_codes.head()}')

    return airport_codes


def read_countries():
   

    data_path = "/Users/juanduque/airflow/data/i94cit&i94res.csv"

    countries = pd.read_csv(data_path)

    logging.info(f'Read Countries::{countries.head()}')

    return countries

def read_visa_codes():
   

    data_path = "/Users/juanduque/airflow/data/i94visa.csv"

    visa_codes = pd.read_csv(data_path)

    logging.info(f'Read Visa Codes::{visa_codes.head()}')

    return visa_codes

def read_travel_mode():
   

    data_path = "/Users/juanduque/airflow/data/i94mode.csv"

    travel_mode = pd.read_csv(data_path)

    logging.info(f'Read Travel::{travel_mode.head()}')

    return travel_mode