import os 
import pandas as pd 
import logging

def clean_immigration(immigration):
  
  df = immigration.drop(['dtadfile', 'visapost', 'matflag'], axis=1)

  df.columns = ['cicid', 
                  'year',
                  'month',
                  'city',
                  'country', 
                  'port',
                  'arrival_date',
                  'mode',
                  'address',
                  'departure_date',
                  'age',
                  'visa',
                  'count',
                  'occupation',
                  'arrival_flag',
                  'departure_flag',
                  'update_flag',
                  'birth_year',
                  'date_allowed_to',
                  'gender',
                  'ins_number',
                  'airline',
                  'admission_number',
                  'flight_number',
                  'visa_type']

  logging.info('Cleaning datetime columns')

  df = df[df['date_allowed_to'].str.len() == 8]

  df = df.drop_duplicates('admission_number')

  df['date_allowed_to'] = pd.to_datetime(df['date_allowed_to'], 
                                           format="%m%d%Y",
                                           errors='coerce')                
  return immigration

def clean_travel_mode(travel_mode):

  logging.info(f'Travel Columns::{travel_mode.columns}')

  travel_mode.columns = [
                        'trans_code',
                        'trans_name',
                          ]

  return travel_mode

def clean_visa_codes(visa_codes):

  logging.info(f'Visa Columns::{visa_codes.columns}')

  visa_codes.columns = [
                        'visa_id',
                        'type',
                          ]

  return visa_codes

def clean_countries(countries):

  logging.info(f'Countries Columns::{countries.columns}')

  countries.columns = ['country_id',
                            'name',
                          ]

  return countries

def clean_demographics(demographics):

    logging.info(f'Demographics Columns::{demographics.columns}')

    demographics.columns = ['city',
                            'state',
                            'median_age',
                            'male_population',
                            'female_population',
                            'total_population',
                            'number_of_veterans',
                            'foreign_born',
                            'average_household_size',
                            'state_code',
                            'race',
                            'count']

    return demographics

def clean_airport_codes(airport_codes):

    airport_codes.columns = ['id_airport',
                             'type', 
                             'name',
                             'elevation_ft',
                             'continent',
                             'iso_country',
                             'iso_region',
                             'municipality',
                             'gps_code',
                             'iata_code',
                             'local_code',
                             'coordinates']

    return airport_codes