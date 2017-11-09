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

Performance:
  Scraper must perform one GET request to traverse the HTML tree for
  every record in a day's worth of arrivals.  Due to code-sharing,
  there are about 2,500 records per day, of which roughly 500 will be
  unique arrivals.

   Connection Speed | Total Time (minutes) | Time per record (sec)
  -----------------------------------------------------------------
    broadband LTE   |      ~ 33 min.       |      ~ 0.77 sec.
        5 Mbps      |      ~ 45 min.       |      ~ 1.02 sec.
"""

# pylint: disable=bad-indentation
from __future__ import print_function

import re
import sqlite3
import sys
import time
import requests

from bs4 import BeautifulSoup
import html5lib


## ====================================================================


# Filename of the customs database.
customs_db = 'customs_db_4.sqlite'

# List of URLs of pages we want to scrape.
base_url = "https://www.airport-jfk.com"

urls = ["https://www.airport-jfk.com/arrivals.php?tp=0",
        "https://www.airport-jfk.com/arrivals.php?tp=6"]
        #"https://www.airport-jfk.com/arrivals.php?tp=12",
        #"https://www.airport-jfk.com/arrivals.php?tp=18"]

# Define the DIV ids for the attributes of interest.
flight_divs = {'parent_div': 'flight_detail',
               'origin': 'fdest',
               'arrival_time': 'fhour',
               'airline': 'fair',
               'flight_num': 'fnum',
               'terminal': 'fterm_mob'}

# Define Metadata for the new table.
arrivals_table_create_query = ('CREATE TABLE IF NOT EXISTS arrivals ('
                                 'id integer primary key, '
                                 'origin text, '
                                 'airport_code text, '
                                 'arrival_time text, '
                                 'airline text, '
                                 'flight_num text,'
                                 'terminal int, '
                                 'code_share text);')

# Define a template SQLite insertion query.
insertion_query = ('INSERT INTO arrivals ('
                     'origin, '
                     'airport_code, '
                     'arrival_time, '
                     'airline, '
                     'flight_num, '
                     'terminal, '
                     'code_share) '
                   'VALUES ('
                     '\'{origin}\', '
                     '\'{airport_code}\', '
                     '\'{arrival_time}\', '
                     '\'{airline}\', '
                     '\'{flight_num}\', '
                     '\'{terminal}\', '
                     '\'{code_share}\');')


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


  def origin(self, flight_div):
    """
    CleanExtractAndVerify Class function for cleaning, extracting, and
    verifying html origin matches.

    Args:
      flight_div: a bs4.element object

    Returns:
      rtn.strip(): string
    """
    # Extract from the flight div.
    origin_result = flight_div.find(id=flight_divs['origin'])

    # Verify search result.
    if origin_result is None: return

    # Clean and Extract non-empty result.
    rtn = origin_result.text
    rtn = re.sub('[ ]*\n[ ]*', '', rtn)
    rtn = re.sub('[ ]*\([A-Z]{3}\)[ ]*', '', rtn)

    # Verify result.
    if re.search(self.origin_ver, rtn): return
    return rtn.strip()


  def airport_code(self, flight_div):
    """
    CleanExtractAndVerify Class function for cleaning, extracting, and
    verifying html airport code matches.

    Args:
      flight_div: a bs4.element object

    Returns:
      rtn.strip(): string
    """
    # Extract from the flight div.
    origin_result = flight_div.find(id=flight_divs['origin'])

    # Verify search result.
    if origin_result is None: return

    # Verify regular expression.
    if not re.search(self.airport_code_match, origin_result.text): return

    # Extract and return attribute.
    rtn = re.compile(self.airport_code_match).search(
                                                origin_result.text).group(0)
    return rtn.strip()


  def arrival_time(self, flight_div):
    """
    CleanExtractAndVerify Class function for cleaning, extracting, and
    verifying html arrival time matches.

    Args:
      flight_div: a bs4.element object

    Returns:
      time_stamp.strip(): string
    """
    # Extract from flight div.
    arrival_time_result = flight_div.find(id=flight_divs['arrival_time'])

    # Verify search result.
    if arrival_time_result is None: return

    # Verify regular expression.
    if not re.search(self.time_stamp_match, arrival_time_result.text): return

    # Clean
    time_stamp = re.compile(self.time_stamp_match).search(
                                            arrival_time_result.text).group(0)
    time_stamp = '0' + time_stamp + ':00' if len(time_stamp) == 4 \
                                          else time_stamp + ':00'
    if re.search('am', arrival_time_result.text, flags=re.IGNORECASE) and \
       time_stamp[0:2] == '12':
      time_stamp = '00' + time_stamp[2:]

    if re.search('pm', arrival_time_result.text, flags=re.IGNORECASE) and \
       int(time_stamp[0:2]) < 12:
      time_stamp = str(int(time_stamp[0:2])+12) + time_stamp[2:]

    return time_stamp.strip()


  def airline(self, flight_div):
    """
    CleanExtractAndVerify Class function for cleaning, extracting, and
    verifying html airline matches.

    Args:
      flight_div: a bs4.element object

    Returns:
      rtn: string
    """
    # Extract from flight div.
    airline_result = flight_div.find(id=flight_divs['airline'])

    # Verify result.
    if airline_result is None: return

    # Clean
    rtn = airline_result.text.strip()

    return rtn


  def flight_num(self, flight_div):
    """
    CleanExtractAndVerify Class function for cleaning, extracting, and
    verifying html flight number matches.

    Args:
      flight_div: a bs4.element object

    Returns:
      rtn: string
    """
    # Extract from flight div.
    flight_num_result = flight_div.find(id=flight_divs['flight_num'])

    # Verify result.
    if flight_num_result is None: return

    # Verify regular expression.
    if re.search(self.flight_num_ver, flight_num_result.text): return

    # Clean
    rtn = flight_num_result.text.strip()

    return rtn


  def terminal(self, flight_div):
    """
    CleanExtractAndVerify Class function for cleaning, extracting, and
    verifying html terminal matches.

    Args:
      flight_div: a bs4.element object

    Returns:
      rtn.strip(): string
    """
    # Extract from flight div.
    terminal_result = flight_div.find(id=flight_divs['terminal'])

    # Verify result.
    if terminal_result is None: return

    # Verify regular expression..
    if not re.search(self.terminal_match, terminal_result.text): return

    #Extract.
    rtn = re.compile(self.terminal_match).search(terminal_result.text).group(0)

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
  total_bad_data = 0
  loaded_flights = 0
  total_records = 0

  # Scrape one url at a time.
  for url_num, url in enumerate(urls):

    # Sleep
    time.sleep(2)

    # Send GET requrest to url and clean up soup with BeautifulSoup.
    request = requests.get(url)
    html = request.text
    soup = BeautifulSoup(html, "html5lib")

    # Extract flight parent DIV.
    flights = soup.findAll(id=flight_divs['parent_div'])
    print ("====================================================")
    print ("Found ", len(flights), " total records in URL #",
           url_num+1, ".", sep="")

    # Initialize some counter variables.
    last_record = (None,None,None)
    url_flights = 0
    url_bad_data = 0

    # Loop through all records.
    for record_num, flight in enumerate(flights):

      # Status update
      total_records += 1

      # Initialize a dic of flight attributes to None.
      flight_attrs = {'origin': None,
                      'airport_code': None,
                      'arrival_time': None,
                      'airline': None,
                      'flight_num': None,
                      'terminal': None,
                      'code_share': 'null'}

      # Extract our tags from the flight parent DIV.
      # Use the CleanExtractAndVerify class to return desired text.
      flight_attrs['origin'] = cleaner.origin(flight)
      flight_attrs['airport_code'] = cleaner.airport_code(flight)
      flight_attrs['arrival_time'] = cleaner.arrival_time(flight)
      flight_attrs['airline'] = cleaner.airline(flight)
      flight_attrs['flight_num'] = cleaner.flight_num(flight)
      flight_attrs['terminal'] = cleaner.terminal(flight)

      # Skip the database insertion if we are missing attributes.
      if None in flight_attrs.values():
        total_bad_data += 1
        url_bad_data += 1
        print (total_records, ': (-) Bad flight found.  Discarding.', sep="")
        continue

      '''
      # Skip the database insertion if we are inserting a duplicate flight.
      if (flight_attrs['origin'],
          flight_attrs['arrival_time'], 
          flight_attrs['terminal']) == last_record:
        total_bad_data += 1
        url_bad_data += 1
        print ('duplicate flight found.')
        continue
        '''

      # Determine if this the operator of the flight, not a code-share flight.
      # If code-share, skip.
      arrival_time_result = flight.find(id=flight_divs['arrival_time'])
      flight_page_link = arrival_time_result.find('a').attrs['href']
      flight_page_html = requests.get(base_url+flight_page_link).text
    
      if re.search('This is a codeshare flight.', flight_page_html):
        flight_page_soup = BeautifulSoup(flight_page_html, "html5lib")
        code_share_div = flight_page_soup.find(id='flight_other').find('a')
        flight_attrs['code_share'] = code_share_div.text
      else:
        flight_attrs['code_share'] = ""

      # Insert into the customs database using SQLite query.
      formatted_query = insertion_query.format(
                                    origin=flight_attrs['origin'],
                                    airport_code=flight_attrs['airport_code'],
                                    arrival_time=flight_attrs['arrival_time'],
                                    airline=flight_attrs['airline'],
                                    flight_num=flight_attrs['flight_num'],
                                    terminal=flight_attrs['terminal'],
                                    code_share=flight_attrs['code_share'])
      cursor.execute(formatted_query)
      connection.commit()
      print(total_records, ": (+) Original flight inserted into database.",
            sep="")

      # Status update
      loaded_flights += 1
      url_flights += 1

    # End of URL.
    print ("====================================================")
    print (url_flights, " (+++) good records in URL #", url_num+1, " loaded.  ",
           loaded_flights, " in total.", sep="")
    print (url_bad_data, " (---) bad and duplicate records in URL #", url_num+1,
           " discarded.", sep="")

  # Clean-up resources.
  connection.commit()
  connection.close()

  # Write out scraping performance.
  print ("====================================================")
  print ("Scraping completed.\n", loaded_flights, " good records loaded.\n",
         total_bad_data, " bad records discarded.\n", "Total time: ",
         int(time.time()-start), " seconds.", sep="")


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

