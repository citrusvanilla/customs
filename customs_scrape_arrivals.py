##
##  JFK Customs Simulation
##  customs_scrapper.py
##
##  Created by Justin Fung on 10/22/17.
##  Copyright 2017 Justin Fung. All rights reserved.
##
## ====================================================================
"""
Adds a table of airplane arrivals to JFK from the JFK website to an
SQLite database. Can be invoked from the command line through the
following:

> python customs_scraper.py

Usage:
  Please see README for how to compile the program and run the
  model and data formatting requirements.
"""
# pylint: disable=bad-indentation
from __future__ import print_function

import re
import sqlite3
import time
import requests

from bs4 import BeautifulSoup
import html5lib

## ====================================================================

# Filename of the customs database.
customs_db = 'customs_db_test.sqlite'

# List of URLs of pages we want to scrape.
urls = ["https://www.airport-jfk.com/arrivals.php?tp=0",
        "https://www.airport-jfk.com/arrivals.php?tp=6",
        "https://www.airport-jfk.com/arrivals.php?tp=12",
        "https://www.airport-jfk.com/arrivals.php?tp=18"]

# Define the DIV ids for the attributes of interest.
flight_divs = {'parent_div': 'flight_detail',
               'origin': 'fdest',
               'arrival_time': 'fhour',
               'airline': 'fair',
               'flight_num': 'fnum',
               'terminal': 'fterm_mob'}

# Define Metadata for the new table.
arrivals_table_create_query = ('CREATE TABLE arrivals ('
                                 'origin text, '
                                 'airport_code text, '
                                 'arrival_time text, '
                                 'airline text, '
                                 'flight_num text,'
                                 'terminal int);')

# Define a template SQLite insertion query.
insertion_query = ('INSERT INTO arrivals ('
                     'origin, '
                     'airport_code, '
                     'arrival_time, '
                     'airline, '
                     'flight_num, '
                     'terminal) '
                   'VALUES ('
                     '\'{origin}\', '
                     '\'{airport_code}\', '
                     '\'{arrival_time}\', '
                     '\'{airline}\', '
                     '\'{flight_num}\', '
                     '\'{terminal}\');')

## ====================================================================

class CleanExtractAndVerify(object):
  """
  Handler Class for cleaning, extracting, and verifying desired arrivals
  database attributes.  Uses re library.

  Member Data:
    self.airport_code_match: regexp string for extracting airport codes
    self.terminal_match: regexp string for extracting JFK terminal number
    self.time_stamp_match: regexp string for extracting arrival time
    self.origin_ver: regexp string for verifying extracted origin
    self.flight_num_ver: regexp string for verifying extracted flight
                         number

  Member Functions:
    origin: clean, extract, and verify html origin match
    airport_code: clean, extract, and verify html airport code match
    arrival_time: clean, extract, and verify html arrival time match
    airline: clean, extract, and verify html airline match
    flight_num: clean, extract, and verify html flight number match
    terminal: clean, extract, and verify html terminal match
  """
  def __init__(self):
    """
    CleanExtractAndVerify Class initializer.  Define regexp here.
    """
    self.airport_code_match = '\([A-Z]{3}\)'
    self.terminal_match = '[0-9]{1}'
    self.time_stamp_match = '[0-9]{1,2}:[0-9]{2}'
    self.origin_ver = '[^ /a-zA-Z.]'
    self.flight_num_ver = '[^ A-Z0-9.]'

  def origin(self, result_set):
    """
    CleanExtractAndVerify Class function for cleaning, extracting, and
    verifying html origin matches.

    Args:
      result_set: a bs4.element.Tag object

    Returns:
      rtn.strip(): string
    """
    # Verify search result.
    if result_set is None: return

    # Clean and Extract non-empty result.
    rtn = result_set.text
    rtn = re.sub('[ ]*\n[ ]*', '', rtn)
    rtn = re.sub('[ ]*\([A-Z]{3}\)[ ]*', '', rtn)

    # Verify result.
    if re.search(self.origin_ver, rtn): return
    return rtn.strip()

  def airport_code(self, result_set):
    """
    CleanExtractAndVerify Class function for cleaning, extracting, and
    verifying html airport code matches.

    Args:
      result_set: a bs4.element.Tag object

    Returns:
      rtn.strip(): string
    """
    # Verify search result.
    if result_set is None: return

    # Verify regular expression.
    if not re.search(self.airport_code_match, result_set.text): return

    # Extract and return attribute.
    rtn = re.compile(self.airport_code_match).search(result_set.text).group(0)
    return rtn.strip()

  def arrival_time(self, result_set):
    """
    CleanExtractAndVerify Class function for cleaning, extracting, and
    verifying html arrival time matches.

    Args:
      result_set: a bs4.element.Tag object

    Returns:
      time_stamp.strip(): string
    """
    # Verify search result.
    if result_set is None: return

    # Verify regular expression.
    if not re.search(self.time_stamp_match, result_set.text): return

    # Clean
    time_stamp = re.compile(self.time_stamp_match).search(result_set.text).group(0)
    time_stamp = '0' + time_stamp + ':00' if len(time_stamp) == 4 \
                                          else time_stamp + ':00'
    if re.search('am', result_set.text, flags=re.IGNORECASE) and time_stamp[0:2] == '12':
        time_stamp = '00' + time_stamp[2:]

    if re.search('pm', result_set.text, flags=re.IGNORECASE):
        time_stamp = str(int(time_stamp[0:2])+12) + time_stamp[2:]

    return time_stamp.strip()

  def airline(self, result_set):
    """
    CleanExtractAndVerify Class function for cleaning, extracting, and
    verifying html airline matches.

    Args:
      result_set: a bs4.element.Tag object

    Returns:
      rtn: string
    """
    # Verify result.
    if result_set is None: return

    # Clean
    rtn = result_set.text.strip()

    return rtn

  def flight_num(self, result_set):
    """
    CleanExtractAndVerify Class function for cleaning, extracting, and
    verifying html flight number matches.

    Args:
      result_set: a bs4.element.Tag object

    Returns:
      rtn: string
    """
    # Verify result.
    if result_set is None: return

    # Verify regular expression.
    if re.search(self.flight_num_ver, result_set.text): return

    # Clean
    rtn = result_set.text.strip()

    return rtn

  def terminal(self, result_set):
    """
    CleanExtractAndVerify Class function for cleaning, extracting, and
    verifying html terminal matches.

    Args:
      result_set: a bs4.element.Tag object

    Returns:
      rtn.strip(): string
    """
    # Verify result.
    if result_set is None: return

    # Verify regular expression..
    if not re.search(self.terminal_match, result_set.text): return

    #Extract.
    rtn = re.compile(self.terminal_match).search(result_set.text).group(0)

    return rtn.strip()

## ====================================================================

def create_arrivals_table(database):
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
  cursor.execute(arrivals_table_create_query)

  # Commit chnages and clean up resources.
  connection.commit()
  connection.close()

def scrape_arrivals(database, urls):
  """
  The main function for scraping the JFK arrivals website of arrivals.
  Uses re, requets, time, sqlite3, html5lib, and BeautifulSoup libraries.

  Args:
    database: string representing database filename
    urls: list of URLs to scrape

  Returns:
    VOID
  """
  # Update the user.
  print ("Connection to DB established.  Scraping ", len(urls),
         " URLs into ", customs_db, "...\n", sep="")
  start = time.time()

  # Open a connection to the database.
  connection = sqlite3.connect(database)
  cursor = connection.cursor()

  # Initialize a CleanExtractAndVerify class for the flight attributes.
  cleaner = CleanExtractAndVerify()

  # Initialize some counters for status updates.
  bad_data = 0
  loaded_flights = 0

  # Scrape one url at a time.
  for url_num, url in enumerate(urls):

    # Send GET requrest to url and clean up soup with BeautifulSoup.
    request = requests.get(url)
    html = request.text
    soup = BeautifulSoup(html, "html5lib")

    # Extract flight parent DIV.
    flights = soup.findAll(id=flight_divs['parent_div'])

    # Loop through all results of the parent DIV.
    num_flights = len(flights)
    url_flights = 0
    print ("Found ", num_flights, " records in URL #", url_num+1, "...", sep="")

    for record_num, flight in enumerate(flights):

      # Initialize a dic of flight attributes to None.
      flight_attrs = {'origin': None,
                      'airport_code': None,
                      'arrival_time': None,
                      'airline': None,
                      'flight_num': None,
                      'terminal': None}

      # Extract our tags from the flight parent DIV.
      origin_result = flight.find(id=flight_divs['origin'])
      arrival_time_result = flight.find(id=flight_divs['arrival_time'])
      airline_result = flight.find(id=flight_divs['airline'])
      flight_num_result = flight.find(id=flight_divs['flight_num'])
      terminal_result = flight.find(id=flight_divs['terminal'])

      # Use the CleanExtractAndVerify class to return desired text.
      flight_attrs['origin'] = cleaner.origin(origin_result)
      flight_attrs['airport_code'] = cleaner.airport_code(origin_result)
      flight_attrs['arrival_time'] = cleaner.arrival_time(arrival_time_result)
      flight_attrs['airline'] = cleaner.airline(airline_result)
      flight_attrs['flight_num'] = cleaner.flight_num(flight_num_result)
      flight_attrs['terminal'] = cleaner.terminal(terminal_result)

      # Skip the database insertion if we are missing attributes.
      if None in flight_attrs.values():
        bad_data += 1
        continue

      # Insert into the customs database using SQLite query.
      formatted_query = insertion_query.format(
                                    origin=flight_attrs['origin'],
                                    airport_code=flight_attrs['airport_code'],
                                    arrival_time=flight_attrs['arrival_time'],
                                    airline=flight_attrs['airline'],
                                    flight_num=flight_attrs['flight_num'],
                                    terminal=flight_attrs['terminal'])
      cursor.execute(formatted_query)

      # Status update
      loaded_flights += 1
      url_flights += 1
      if loaded_flights % 100 == 0:
        print ("Loaded ", loaded_flights,
               " total flights so far into the database...", sep="")

    # End of URL.
    print (url_flights, " records in URL #", url_num+1, " loaded.\n", sep="")

  # Clean-up resources.
  connection.commit()
  connection.close()

  # Write out scraping performance.
  print ("Scraping completed.\n", loaded_flights, " good records loaded.\n",
         bad_data, " bad records discarded.\n", "Total time: ",
         int(time.time()-start), " seconds.\n", sep="")

## ====================================================================

def main():
  """
  Main.  Invoked from command line with no arguments.  Creates table in
  SQLite database and inserts web-scrapped data into the database.

  Args:
    None
  
  Returns:
    VOID
  """
  # Create table.
  create_arrivals_table(customs_db)

  # Scrape them!
  scrape_arrivals(customs_db, urls)

if __name__ == "__main__": 
  main()

