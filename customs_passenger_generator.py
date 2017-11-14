##
##  JFK Customs Simulation
##  customs_passenger_generator.py
##
##  Created by Justin Fung on 10/22/17.
##  Copyright 2017 Justin Fung. All rights reserved.
##
## ====================================================================
# pylint: disable=bad-indentation,bad-continuation,multiple-statements
# pylint: disable=invalid-name

"""
Adds a table of passengers to a customs database.  Can be invoked from
the command line through the following:

> python customs_passenger_generator.py

Usage:
  Please see README for how to compile the program and run the
  model and data formatting requirements.
"""

from __future__ import print_function

from faker import Faker
import sqlite3
import random
import re

import numpy as np


## ====================================================================


customs_db = "customs_db.sqlite"

passengers_table_create_query = ('CREATE TABLE IF NOT EXISTS passengers ('
                                   'id integer PRIMARY KEY, '
                                   'flight_num text, '
                                   'first_name text, '
                                   'last_name text, '
                                   'birthdate text, '
                                   'nationality text, '
                                   'enque_time text, '
                                   'departure_time text, '
                                   'service_time text, '
                                   'connecting_flight bool, '
                                   'processed bool);')

insertion_query = ('INSERT INTO passengers ('
                     'flight_num, '
                     'first_name, '
                     'last_name, '
                     'birthdate, '
                     'nationality) '
                   'VALUES ('
                     '\'{flight_num}\', '
                     '\'{first_name}\', '
                     '\'{last_name}\', '
                     '\'{birthdate}\', '
                     '\'{nationality}\');')


## ====================================================================


def create_passengers_table(database):
  """
  Function for creating a table in SQLite.

  Args:
    database: a string representing database filename.

  Returns:
    VOID
  """
  # Open a connection to the database.
  connection = sqlite3.connect(database)
  cursor = connection.cursor()

  # Build the table according to the SQLite query.
  cursor.execute(passengers_table_create_query)

  # Commit chnages and clean up resources.
  connection.commit()
  connection.close()  


def guess_seat_count(plane_list):
  ''''''
  passenger_count = 0
  plane_count = 0

  for plane in plane_list:
    if int(plane[0]) > 0:
      passenger_count += int(plane[0])
      plane_count += 1

  if plane_count == 0:
    average = 0
  else:
    average = int(passenger_count / plane_count)

  return average


def generate_nationality_distribution():
  ''''''
  return np.random.triangular(0.3,0.4,0.5)


def generate_nationality(probability):
  ''''''
  if random.random() < probability:
    return "domestic"
  else:
    return "foreign"


def reformat_code_share(string):
  ''''''
  if string != "" and \
     re.search("[A-Z]+", string) is not None and \
     re.search("[0-9]+", string) is not None:
    return (re.search("[A-Z]+", string).group(0) + 
            " " + 
            re.search("[0-9]+", string).group(0))
  else:
    return ""


def insert_passengers(database, flight_num, total_seats):
  ''''''
  #connection = sqlite3.connect(database)
  #cursor = connection.cursor()

  

  #connection.commit()
  #connection.close()


def fake_passengers(database):
  ''''''
  # Establish connection to the database.
  connection = sqlite3.connect(customs_db)
  cursor = connection.cursor()

  # Build the passengers database if it does not exist.
  create_passengers_table(database)

  # Initialize a faker generator.
  fake = Faker()

  # Retrieve arrivals.
  cursor.execute('SELECT * '
                   'FROM arrivals;')
  arrivals = cursor.fetchall()

  inserted = 0

  # Loop through arrivals to get plane types.
  for idx, arrival in enumerate(arrivals):
    print ("Working on arrival ", idx, ".", sep="")

    flight_num = arrival[5]
    code_share = arrival[7]
    total_seats = None
    nationality_distribution = generate_nationality_distribution()

    # Duplicate detection is done by checking for code shares.
    if code_share == "":

      # Get the aircraft and seat count from the planes table.
      rslt = cursor.execute('SELECT aircraft, total_seats '
                              'FROM planes '
                              'WHERE flight_num = \'{flight_num}\';'\
                                 .format(flight_num=flight_num)).fetchone()
      if rslt is not None:
        aircraft = str(rslt[0])
        total_seats = int(rslt[1])
        # If we have missing seat count, we average the counts for the
        # same plane types.
        if total_seats <= 0:
          rslt2 = cursor.execute('SELECT total_seats '
                                'FROM planes '
                                'WHERE aircraft = \'{aircraft}\';'\
                                     .format(aircraft=aircraft)).fetchall()
          total_seats = guess_seat_count(rslt2)

        # Fake the data.
        for passenger in range(total_seats):
          passenger_info = {}
          passenger_info['flight_num'] = flight_num
          passenger_info['first_name'] = fake.first_name()
          passenger_info['last_name'] = fake.last_name()
          passenger_info['birthdate'] = fake.year()
          passenger_info['nationality'] = generate_nationality(
                                              nationality_distribution)

          # Insert into database.
          cursor.execute(insertion_query.format(
                                        flight_num=passenger_info['flight_num'],
                                        first_name=passenger_info['first_name'],
                                        last_name=passenger_info['last_name'],
                                        birthdate=passenger_info['birthdate'],
                                        nationality=passenger_info['nationality']))

        inserted += 1
        print("Inserted ", total_seats, " passengers into the database for flight ",
              flight_num, ". (", inserted, " planes in total.)", sep="")
        connection.commit()

    else:
      rslt3 = cursor.execute('SELECT * from passengers ' 
                               'WHERE flight_num = \'{code_share}\';'\
                                    .format(code_share=code_share)).fetchone()

      if rslt3 is not None: continue

      # Get the aircraft and seat count from the planes table.
      rslt4 = cursor.execute('SELECT aircraft, total_seats '
                                 'FROM planes '
                                 'WHERE flight_num = \'{flight_num}\';'\
                                   .format(flight_num=flight_num)).fetchone()
      if rslt4 is not None:
        aircraft = str(rslt4[0])
        total_seats = int(rslt4[1])
        
        # If we have missing seat count, we average the counts for the
        # same plane types.
        if total_seats <= 0:
          rslt5 = cursor.execute('SELECT total_seats '
                                   'FROM planes '
                                   'WHERE aircraft = \'{aircraft}\';'\
                                     .format(aircraft=aircraft)).fetchall()
          total_seats = guess_seat_count(rslt5)

        # Fake the data.
        for passenger in range(total_seats):
          passenger_info = {}
          passenger_info['flight_num'] = code_share
          passenger_info['first_name'] = fake.first_name()
          passenger_info['last_name'] = fake.last_name()
          passenger_info['birthdate'] = fake.year()
          passenger_info['nationality'] = generate_nationality(
                                              nationality_distribution)

          # Insert into database.
          cursor.execute(insertion_query.format(
                                        flight_num=passenger_info['flight_num'],
                                        first_name=passenger_info['first_name'],
                                        last_name=passenger_info['last_name'],
                                        birthdate=passenger_info['birthdate'],
                                        nationality=passenger_info['nationality']))

        inserted += 1
        print("Inserted ", total_seats, " passengers into the database for flight ",
              flight_num, ". (", inserted, " planes in total.)", sep="")
        connection.commit()


## ====================================================================


def main():
  """
  Main.  Invoked from command line.

  Args:
    None

  Returns:
    VOID
  """
  # Get the database name.
  fake_passengers(customs_db)


if __name__ == "__main__":
  main()

