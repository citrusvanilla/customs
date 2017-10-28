##
##  JFK Customs Simulation
##  customs_scrapper.py
##
##  Created by Justin Fung on 10/22/17.
##  Copyright 2017 Justin Fung. All rights reserved.
##
## ====================================================================
"""
"""
# pylint: disable=bad-indentation
from __future__ import print_function

import re
import sqlite3
import requests
from bs4 import BeautifulSoup
import html5lib

## ====================================================================

# Filename of the customs database.
customs_db = 'customs_db_test.sqlite'

# List of URLs of pages we want to scrape.
urls = ["https://www.airport-jfk.com/arrivals.php?tp=" + i
                                for i in ["0", "6", "12", "18"]]

# Define the DIV ids for the attributes of interest.
flight_divs = {'parent_div': 'flight_detail',
               'origin': 'fdest',
               'arrival_time': 'fhour',
               'airline': 'fair',
               'flight_num': 'fnum',
               'terminal': 'fterm_mob'}

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
  """
  def __init__(self):
    """
    """
    self.airport_code_match = '\([A-Z]{3}\)'
    self.terminal_match = '[0-9]{1}'
    self.time_stamp_match ='[0-9]{1,2}:[0-9]{2}'
    self.origin_ver = '[^ a-zA-Z.]'
    self.flight_num_ver = '[^ A-Z0-9.]'

  def origin(self, result_set):
    """
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

    return time_stamp.strip()

  def airline(self, result_set):
    """
    """
    # Verify result.
    if result_set is None: return

    # Clean
    rtn = result_set.text.strip()

    return rtn

  def flight_num(self, result_set):
    """
    """
    # Verify result.
    if result_set is None: return

    # Verify regular expression.
    if re.search(self.flight_num_ver, result_set.text): return

    return result_set.text.strip()

  def terminal(self, result_set):
    """
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
  """
  connection = sqlite3.connect(database)
  cursor = connection.cursor()

  # Define Metadata for the tables.
  arrivals_table_create_query = ('CREATE TABLE arrivals ('
                                   'origin text, '
                                   'airport_code text, '
                                   'arrival_time text, '
                                   'airline text, '
                                   'flight_num text,'
                                   'terminal int);')

  cursor.execute(arrivals_table_create_query)
  connection.commit()
  connection.close()

def scrape_arrivals(database, urls):
  """
  """
  # Open a connection to the database.
  connection = sqlite3.connect(database)
  cursor = connection.cursor()
  print ("Connection to DB established.  Scraping ", len(urls),
         " URLs into ", customs_db, "...", sep="")

  # Initialize a CleanAndExtract class.
  cleaner = CleanExtractAndVerify()

  # Initialize some counters for status updates.
  bad_data = 0
  loaded_flights = 0

  # Scrape one url at a time.
  for idx, url in enumerate(urls):

    # Send GET requrest to url and clean up soup with BeautifulSoup.
    request = requests.get(url)
    html = request.text
    soup = BeautifulSoup(html, "html5lib")

    # Extract flight parent DIVs.
    flights = soup.findAll(id=flight_divs['parent_div'])

    # Loop through all results of the parent DIV.
    num_flights = len(flights)
    print ("Found ", num_flights, " records in URL #", idx+1, "...", sep="")

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

      # Use the CleanAndExtract class to return desired text.
      flight_attrs['origin'] = cleaner.origin(origin_result)
      flight_attrs['airport_code'] = cleaner.airport_code(origin_result)
      flight_attrs['arrival_time'] = cleaner.arrival_time(arrival_time_result)
      flight_attrs['airline'] = cleaner.airline(airline_result)
      flight_attrs['flight_num'] = cleaner.flight_num(flight_num_result)
      flight_attrs['terminal'] = cleaner.terminal(terminal_result)

      # Skip the result entirely if we are missing any of the following:
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
      if loaded_flights % 100 == 0:
        print ("Loaded ", loaded_flights, " flights into ",
               customs_db, "...", sep="")

    # End of URL.
    print ("URL #", idx+1," loaded. ", bad_data, " records discarded in total.", sep="")

  # Clean-up resources.
  connection.commit()
  connection.close()

  print ("Scraping complete. ", loaded_flights, " good records loaded.", sep="")

## ====================================================================

def main():
  """
  """
  # Create table.
  create_arrivals_table(customs_db)

  # Scrape them!
  scrape_arrivals(customs_db, urls)

if __name__ == "__main__":
  main()
