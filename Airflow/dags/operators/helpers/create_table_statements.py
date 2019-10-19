def drop_table_if_exists(table_name):
    """
    returns a sql statement to drop the immigration table if it exists.
    parameters:
        - table_name: <string> representing the table to be dropped.
    returns:
        - sql_statement: <string> drop table sql statement.
    """

    return f"""
    DROP TABLE IF EXISTS {table_name}
    """

## create immigrations table command.
create_immigrations_table = """
CREATE TABLE IF NOT EXISTS immigrations (
    cicid  INTEGER NOT NULL,
    year INTEGER,
    month INTEGER,
    city INTEGER,
    country INTEGER,
    port VARCHAR,
    arrival_date timestamp,
    mode INTEGER,
    address VARCHAR,
    departure_date timestamp,
    age INTEGER,
    visa INTEGER,
    count INTEGER,
    occupation VARCHAR,
    arrival_flag VARCHAR,
    departure_flag VARCHAR,
    update_flag VARCHAR,
    birth_year INTEGER,
    date_allowed_to VARCHAR,
    gender VARCHAR,
    ins_number VARCHAR,
    airline VARCHAR,
    admission_number BIGINT NOT NULL UNIQUE,
    flight_number VARCHAR,
    visa_type VARCHAR,
    PRIMARY KEY(cicid)
) 
DISTKEY (country) 
SORTKEY (arrival_date);
"""


create_countries = """
CREATE TABLE IF NOT EXISTS countries (
    country_id INTEGER NOT NULL IDENTITY(0,1),
    country VARCHAR,
    PRIMARY KEY (country_id)
)
diststyle all;
"""

create_demographics = """
CREATE TABLE IF NOT EXISTS demographics (
    city VARCHAR NOT NULL,
    median_age INTEGER,
    male_population INTEGER,
    female_population INTEGER,
    total_population INTEGER,
    number_of_veterans INTEGER, 
    foreign_born INTEGER,
    average_household_size FLOAT,
    race VARCHAR,
    count INTEGER,
    state VARCHAR,
    state_code VARCHAR,
    PRIMARY KEY (city)
)
diststyle all;
"""

create_cities = """
CREATE TABLE IF NOT EXISTS cities (
    city_id INTEGER NOT NULL IDENTITY(0,1),
    name VARCHAR,
    state VARCHAR,
    state_code VARCHAR,
    PRIMARY KEY (city_id)
) 
diststyle all;
"""

create_airport_codes = """
CREATE TABLE IF NOT EXISTS airport_codes (
    id_airport VARCHAR UNIQUE NOT NULL,
    type VARCHAR,
    name VARCHAR,
    elevation_ft FLOAT,
    continent VARCHAR,
    iso_country VARCHAR,
    iso_region VARCHAR,
    municipality VARCHAR,
    gps_code VARCHAR,
    iata_code VARCHAR,
    local_code VARCHAR,
    coordinates VARCHAR,
    PRIMARY KEY (id_airport)
)
diststyle all;
"""

create_visa_codes = """
CREATE TABLE IF NOT EXISTS visa_codes (
    visa_id VARCHAR UNIQUE NOT NULL,
    type VARCHAR,
    PRIMARY KEY (visa_id)
)
diststyle all;
"""

create_travel_mode = """
CREATE TABLE IF NOT EXISTS travel_mode (
    travel_id VARCHAR UNIQUE NOT NULL,
    transportation VARCHAR,
    PRIMARY KEY (travel_id)
)
diststyle all;
"""

create_statements = {
    'immigrations': create_immigrations_table,
    'countries': create_countries,
    'demographics': create_demographics,
    'cities': create_cities,
    'airport_codes': create_airport_codes,
    'visa_codes': create_visa_codes,
    'travel_mode': create_travel_mode
}