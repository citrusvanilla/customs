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

Usage:
  Please see the README for how to compile the program and run the
  model and CSV formatting requirements.
"""
import sqlite3
import pandas

# Customs Database Metadata:
sqlite_file = "customs_db.sqlite"

# Schedule filepaths.
arrival_schedule_csvfile = "schedules/sample_arrival_schedule.csv"
server_schedule_csvfile = "schedules/sample_server_schedule.csv"


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
				  				   'id integer PRIMARY KEY, '
								   'arrival_time text, '
								   'passengers_dom integer, '
								   'passengers_intl integer) '
								   'WITHOUT ROWID;')

  servers_table_create_query = ('CREATE TABLE servers ('
							      'id integer PRIMARY KEY, '
								  'subsection text, '
								  '\'0-4\' integer, '
								  '\'4-8\' integer, '
								  '\'8-12\' integer, '
								  '\'12-16\' integer, '
								  '\'16-20\' integer, '
								  '\'20-24\' integer) '
								   'WITHOUT ROWID;')

  # Create new SQLite tables.
  cursor.execute(arrivals_table_create_query)
  cursor.execute(servers_table_create_query)

  # Use Pandas to commit a CSV to the database with an intermediate.
  df_arrivals = pandas.read_csv(arrival_schedule_csvfile)
  df_servers = pandas.read_csv(server_schedule_csvfile)

  df_arrivals.to_sql("arrivals", connection, if_exists='append', index=False)
  df_servers.to_sql("servers", connection, if_exists='append', index=False)

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

