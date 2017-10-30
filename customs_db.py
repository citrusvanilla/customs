##
##  JFK Customs Simulation
##  customs_db.py
##
##  Created by Justin Fung on 10/22/17.
##  Copyright 2017 Justin Fung. All rights reserved.
##
## ====================================================================

"""
Builds an SQLite database to local disk to hold program data.
Can be invoked from the command line through the following:

> python customs_db.py

Usage:
  Please see README for how to compile the program and run the
  model and data formatting requirements.

Relevant DOCS:
  - https://faker.readthedocs.io/en/latest/providers/faker.providers.person.html
"""
import sqlite3
import pandas

from faker import Faker

# Customs Database Metadata:
sqlite_file = "customs_db.sqlite"

# Schedule filepaths.
arrival_schedule_csvfile = "schedules/sample_arrival_schedule.csv"
server_schedule_csvfile = "schedules/sample_server_schedule.csv"

# Data filepaths.
airports_data_csvfile = "data/airports.csv"

def build(sqlite_file, arrivals_csv, servers_csv):
  """
  Function that builds an initialized SQLite database file to disk.

  Args:

  Returns:
    VOID
  """
  # Create Database by opening a connection for the first time.
  connection = sqlite3.connect(sqlite_file)
  cursor = connection.cursor()

  # Define Metadata for the tables.
  arrivals_table_create_query = ('CREATE TABLE arrivals ('
				  				   'plane_id integer PRIMARY KEY, '
								   'arrival_time text, '
								   'airline text, '
								   'flight_number) '
								   'WITHOUT ROWID;')

  servers_table_create_query = ('CREATE TABLE servers ('
							      'server_id integer PRIMARY KEY, '
								  'subsection text, '
								  '\'0-4\' integer, '
								  '\'4-8\' integer, '
								  '\'8-12\' integer, '
								  '\'12-16\' integer, '
								  '\'16-20\' integer, '
								  '\'20-24\' integer) '
								  'WITHOUT ROWID;')

  passengers_table_create_query = ('CREATE TABLE passengers ('
							        'passenger_id integer PRIMARY KEY, '
							        'plane_id FOREIGN KEY, '
								    'first_name text, '   #fake.first_name()
								    'last_name integer, '  #fake.last_name()
								    'birth_date integer, '  #fake.date_between(start_date="-70y", end_date="-4y")
								    'nationality integer) '  #fake.country()
								    'WITHOUT ROWID;')

  airports_table_create_query = ('CREATE TABLE airports ('
  	     						   'code text PRIMARY KEY, '
  	     						   'name text, '
  	     						   'city text, '
  	     						   'country text);')

  # Create new SQLite tables.
  cursor.execute(arrivals_table_create_query)
  cursor.execute(servers_table_create_query)
  cursor.execute(passengers_table_create_query)
  cursor.execute(airports_table_create_query)

  # Use Pandas to commit a CSV to the database with an intermediate.
  df_arrivals = pandas.read_csv(arrival_schedule_csvfile)
  df_servers = pandas.read_csv(server_schedule_csvfile)
  df_airports = pandas.read_csv(airports_data_csvfile)

  df_arrivals.to_sql("arrivals", connection, if_exists='append', index=False)
  df_servers.to_sql("servers", connection, if_exists='append', index=False)
  df_airports.to_sql("airports", connection, if_exists='append', index=False)

  # Generate fake people for the database.
  dom_intl_split_perc = 0.5

  passenger_id = 1

  for index, row in df_arrivals.iterrows():
  	plane_id = row[0]
  	first_name = fake.first_name()
  	last_name = fake.last_name()
  	birth_date = fake.date_between(start_date="-70y", end_date="-4y")
  	nationality = fake.country() if random.random() <= dom_intl_split_perc 
  	 							 else 'United States of America'





  # Commit changes and close the connection to the database file.
  connection.commit()
  connection.close()


def main():
  """
  Main.

  Args:
    None
  """
  # Build it!
  build (sqlite_file, arrival_schedule_csvfile, server_schedule_csvfile)


if __name__ == "__main__":
  main()

