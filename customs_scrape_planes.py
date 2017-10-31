##
##  JFK Customs Simulation
##  customs_scrape_planes.py
##
##  Created by Justin Fung on 10/22/17.
##  Copyright 2017 Justin Fung. All rights reserved.
##
## ====================================================================
# pylint: disable=bad-indentation,bad-continuation,multiple-statements
# pylint: disable=invalid-name
"""
Adds a table of airplanes with carrier, aircraft, and number of seats
to an SQLite database. Can be invoked from the command line through the
following:

> python customs_scrape_planes.py

Usage:
  Please see README for how to compile the program and run the
  model and data formatting requirements.

Helpful Debugging Commands:
  driver.save_screenshot('image.png') -> screenshots your driver's
                                         current screen.

Performance:


TODO:
  - complete the fill_search_form_and_submit error handling
  - Restart from 850.
"""
from __future__ import print_function

from datetime import datetime
import re
import sqlite3
import sys

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

import html5lib


## ====================================================================


# Customs database filepath.
customs_db = "customs_db_test.sqlite"

# Webdriver binary/executable filepath.
#webdriver_exe = '/usr/local/phantomjs-2.1.1-macosx/bin/phantomjs'
webdriver_exe = '/usr/local/bin/chromedriver'

# URL we want to scrape.
url = "https://www.seatguru.com/findseatmap/findseatmap.php"

# SQLite query for creating a 'planes' table in an SQLite database.
create_planes_table_query = ('CREATE TABLE planes ('
                               'flight_num text, '
                               'carrier text, '
                               'aircraft text, '
                               'total_seats text);')

# SQLite template query for inserting planes into the database.
insertion_query = ('INSERT INTO planes ('
                     'flight_num, '
                     'carrier, '
                     'aircraft, '
                     'total_seats) '
                   'VALUES ('
                     '\'{flight_num}\', '
                     '\'{carrier}\', '
                     '\'{aircraft}\', '
                     '\'{total_seats}\');')


## ====================================================================


def load_driver(webdriver_exe, url):
  """
  Starts up a webdriver and loads a passed URL.

  Args:
    webdriver_exe: filepath to webdriver binary/executable
    url: URL address as string

  Returns:
    driver: a initialized selenium webdriver object
  """
  # Use PhantomJS headless browser.
  print("Loading PhantomJS webdriver...")
  #driver = webdriver.PhantomJS(webdriver_exe)
  driver = webdriver.Chrome(webdriver_exe)

  # Default implicit WAIT command for all AJAX and JS loading.
  driver.implicitly_wait(2) # seconds

  # Retrieve the page.
  driver.get(url)

  # Status update.
  print("PhantomJS driver \'", webdriver_exe, " \' initialized.\n",
        "Driver has loaded \'", url, "\'.", sep="")

  # Return driver control to main.
  return driver


def create_planes_table(database):
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

  # TROUBLESHOOTING
  cursor.execute('DROP TABLE IF EXISTS planes;')

  # Build the table according to the SQLite query.
  cursor.execute(create_planes_table_query)

  # Commit chnages and clean up resources.
  connection.commit()
  connection.close()


def fill_search_form_and_submit(driver, flight_attrs):
  """
  Completes the main search form at SeatGuru.com and submit it.

  Args:
    driver: an initialized selenium webdriver object
    flight_attrs: a dictionary containing attributes for the form

  Returns:
    success: boolean indicating presence of successful search result
  """
  print ("Searching SeatGuru for flight ", flight_attrs['flight_num'], " ",
         flight_attrs['carrier'], ".", sep="")
  # boolean for success
  success = True

  # Fill out the carrier name on the front page first.
  airline_field = driver.find_element_by_id('airline-select')
  airline_field.clear()
  carrier_name = flight_attrs['carrier'].split(" ")
  airline_field.send_keys(" ".join(carrier_name))
  airline_autocomplete = driver.find_element_by_css_selector(
           ".ui-autocomplete.ui-menu.ui-widget.ui-widget-content.ui-corner-all")
  try:
    WebDriverWait(driver,1).until(EC.visibility_of(airline_autocomplete))
  except:
    pass

  # If the database carrier name has not triggered the autocomplete form,
  # we should shorten the name and try again.
  while not airline_autocomplete.is_displayed() and len(carrier_name) > 1:
    airline_field.clear()
    carrier_name = carrier_name[0: len(carrier_name) - 1]
    airline_field.send_keys(" ".join(carrier_name))
    try:
      WebDriverWait(driver,1).until(EC.visibility_of(airline_autocomplete))
    except:
      pass

  # If we triggered the airline autocomplete, choose the top result.
  # If we could not trigger the autocomplete, we must return False.
  if airline_autocomplete.is_displayed():
    driver.find_element_by_class_name('ui-autocomplete')\
                   .find_element_by_class_name('ui-corner-all').click()
  else:
    print("(X) Airline is not in SeatGuru database.  Check airline name.")
    success = False
    return success

  # Fill out the flight number on the front page next.
  flight_num = driver.find_element_by_id('flightno')
  flight_num.clear()
  flight_num.send_keys(flight_attrs['flight_num'].split(" ")[-1])

  # Fill out the departure date on the front page next.
  arrival_date = driver.find_element_by_id('datepicker')
  arrival_date.clear()
  arrival_date.send_keys(datetime.today().strftime("%m/%d/%Y"))
  driver.find_element_by_class_name('ui-datepicker-today').click()

  # Submit the form and navigate to the search results.
  driver.find_element_by_id('search').click()

  # Control should now be looking at a results page.
  # If results are not present, go back and try next record.
  if len(driver.find_elements_by_class_name('chooseFlights-row')) == 0:
      success = False
      print("(X) No search results found.")
      return success

  return success


def _get_indices(cursor):
  """
  Helper function for retrieving the indices from the Arrivals table
  of the headers of interest.

  Args:
    cursor: an sqlite cursor initialized from the customs database

  Returns:
    indices: the indices of the headers of interest
  """
  # Get the indices from the arrivals table that we need.
  headers = cursor.execute('PRAGMA table_info(arrivals)').fetchall()

  # Initialize a dictionary to hold relevant indexes.
  indices = {'origin': None,
         'airline': None,
         'flight_num': None,
         'arrival_time': None}
         #'date': None}

  # Find the indices and store them.
  for idx, tup in enumerate(headers):
    if tup[1] == "origin":
      indices['origin'] = idx
    elif tup[1] == "airline":
      indices['airline'] = idx
    elif tup[1] == "flight_num":
      indices['flight_num'] = idx
    elif tup[1] == "arrival_time":
      indices['arrival_time'] = idx
    #elif tup[1] == "date":
    # indices['date'] = idx

  # Exit if these indices are not found.
  if None in indices.values():
    raise Exception('Could not match arrivals table headers. '
                  'Check header names.')

  # Return the indices dictionary.
  return indices


def extract_plane_insert_and_return(driver, cursor_planes, flight_attrs):
  """
  Extracts plane data from the plane's page.  Inserts plane data into
  the customes database.

  Args:
    driver: an initialized selenium webdriver
    cursor_planes: a secondary initialized sqlite cursor to operate on
                   the plane table
    flight_attrs: a dictionary to store intermediate plane database

  Returns:
    a boolean indicating successful insertion of plane data
  """
  # Check for three cases: 1. airplane hyperlink, 2. airplane plain text,
  # and 3. no airplane
  try:
    # Grab the top result as a hyperlink. Find the airplane type and link.
    airplane = WebDriverWait(driver,2)\
                             .until(EC.element_to_be_clickable(
                                           (By.CLASS_NAME,'flightno')))
    flight_attrs['aircraft'] = airplane.text
  except:
    # Grab the top result as plaintext.
    airplane_rows = driver.find_elements_by_class_name('chooseFlights-row')
    if len(airplane_rows) == 0:
      print("(X) No airplane types found.")
      return False
    airplane_string = airplane_rows[0]
    match = re.search('[0-9]{1,2}:[0-9]{2}[AaPp]([^:]*)No Map',
                        airplane_string.text)
    
    # If no airplane found at all, exit.
    if not match:
      print("(X) No airplane types found.")
      return False  
    
    # If airplane found in plaintext.
    airplane = match.group(1)
    flight_attrs['aircraft'] = airplane.strip()

  # Check to see if this entry is already in the database.
  # If yes, return false.
  cursor_planes.execute('SELECT * FROM planes '
                          'WHERE carrier = \'{carrier}\' '
                          'AND aircraft = \'{aircraft}\';'\
                                 .format(carrier=flight_attrs['carrier'],
                                         aircraft=flight_attrs['aircraft']))
  if len(cursor_planes.fetchall()) != 0:
    print("(X) Plane already exists in the database.", sep="")
    return False

  try:
    # Navigate to the airplane's page.
    airplane.click()

    # Get all classes corresponding to the seats table.
    seat_list = driver.find_elements_by_class_name('item4')

    # Extract number of seats and assign to plane dictionary.
    num_seats = 0

    for item in seat_list:
      if re.search('^[0-9]{1,}', item.text):
        num_seats += int(re.compile('[0-9]{1,}').search(item.text).group(0))

    flight_attrs['total_seats'] = num_seats

    # Return control
    driver.execute_script("window.history.go(-1)")
  except:
    flight_attrs['total_seats'] = -1

  # Insert into database.
  cursor_planes.execute(insertion_query.format(
                        flight_num=flight_attrs['flight_num'],
                        carrier=flight_attrs['carrier'],
                        aircraft=flight_attrs['aircraft'],
                        total_seats=flight_attrs['total_seats']))

  return True


def scrape_planes(driver, database, initial_record):
  """
  Main routine for scraping plane data into an SQLite database.

  Args:
    driver: an initialized selenium webdriver object
    database: filepath to the customs database

  Returns:
    VOID
  """
  # Open up a connection to the database, initialize a cursor.
  connection = sqlite3.connect(database)
  cursor_arrivals = connection.cursor()
  cursor_planes = connection.cursor()

  # get indices from database.
  indices = _get_indices(cursor_arrivals)

  # Point the cursor at the list of flights for which we want plane data.
  cursor_arrivals.execute('SELECT * FROM arrivals LIMIT -1 OFFSET {idx};'\
                           .format(idx=initial_record))

  # SOME COUNTERS
  inserted_planes = 0
  bad_planes = 0

  # Iterate through every flight.
  for idx, row in enumerate(cursor_arrivals):

    # Status update.
    print("==================================================")
    print("FLIGHT RECORD #", int(initial_record) + idx, ":", sep="")

    # Initiate a dictionary of flight attributes for the web scraping
    # routine.
    flight_attrs = {'carrier': None,
            'flight_num': None,
            'date': None,
            'arrival_time': None,
            'aircraft': None,
            'total_seats': None}

    # Retrieve the carrier, flight number, arrival_time and date from
    # the sqlite query.
    flight_attrs['carrier'] = row[indices['airline']]
    flight_attrs['flight_num'] = row[indices['flight_num']]
    #flight_attrs['date'] = row[indices['date']]
    flight_attrs['arrival_time'] = row[indices['arrival_time']]

    # Fill out the search page.
    found_potential_plane = fill_search_form_and_submit(driver,
                                                        flight_attrs)

    # If our search did not turn up a result, we move to the next record.
    if not found_potential_plane: continue

    # If we have a potential plane, let's extract it, insert it into
    # the DB and return control to the search page.
    extracted = extract_plane_insert_and_return(driver, cursor_planes,
                                                flight_attrs)

    # If we could not extract plane information, move to next record.
    if not extracted: continue

    # Status updates here.
    connection.commit()
    inserted_planes += 1
    print("(*) Found and extracted plane #", inserted_planes, ": ",
          flight_attrs['carrier'], " ", flight_attrs['aircraft'], ".", sep="")

    if inserted_planes % 10 == 0:
      print("==================================================")
      print ("Loaded ", inserted_planes,
             " total planes into the database so far.", sep="")

  # Clean-up resources.
  connection.commit()
  connection.close()


## ====================================================================


def main():
  """
  Main.  Invoked from command line.

  Args:
    None

  Returns:
    VOID
  """
  # Get first index.
  initial_record = sys.argv[1]

  # Load up the webdriver and point it to the top-level URL.
  driver = load_driver(webdriver_exe, url)

  # Create table in SQLite database.
  if initial_record == "0":
    create_planes_table(customs_db)

  # Initiate Scraping.
  scrape_planes(driver, customs_db, initial_record)


if __name__ == "__main__":
  main()
