##
##  JFK Customs Simulation
##  customs_scrape_planes.py
##
##  Created by Justin Fung on 10/22/17.
##  Copyright 2017 Justin Fung. All rights reserved.
##
## ====================================================================
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
"""
from __future__ import print_function

import re
import selenium
from bs4 import BeautifulSoup
import html5lib
import sqlite3

from datetime import datetime
from selenium import webdriver

from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By


## ====================================================================


# Customs database filepath.
customs_db = "customs_db_test.sqlite"

# Webdriver binary/executable filepath.
webdriver_exe = '/usr/local/phantomjs-2.1.1-macosx/bin/phantomjs'

# URL we want to scrape.
url = "https://www.seatguru.com/findseatmap/findseatmap.php"

# SQLite query for creating a planes table in an SQLite database.
create_planes_table_query = ('CREATE TABLE planes ('
                               'carrier text, '
                               'aircraft text, '
                               'total_seats text);')

# SQLite template query for inserting planes into the database.
insertion_query = ('INSERT INTO planes ('
                     'carrier, '
                     'aircraft, '
                     'total_seats) '
                   'VALUES ('
                     '\'{carrier}\', '
                     '\'{aircraft}\', '
                     '\'{total_seats}\');')


## ====================================================================

class PlaneScraper(object):
	pass

## ====================================================================


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
  cursor_arrivals.execute('DROP TABLE IF EXISTS planes;')

  # Build the table according to the SQLite query.
  cursor.execute(create_planes_table_query)

  # Commit chnages and clean up resources.
  connection.commit()
  connection.close()


def load_driver(webdriver_exe, url):
  """
  """
  # Use PhantomJS headless browser.
  driver = webdriver.PhantomJS(webdriver_exe)
  
  # Default implicit WAIT command for all AJAX and JS loading.
  driver.implicitly_wait(5) # seconds

  # Retrieve the page.
  driver.get(url)

  # Status update.
  print("PhantomJS driver \'", webdriver_exe, " \' initialized.\n ",
  	    "Driver has loaded \'", url, "\'.\n", sep="")

  # Return driver control to main.
  return driver


def fill_search_form_and_submit(driver, flight_attrs):
  """
  """
  # boolean for success
  success = True

  # Fill out the carrier name on the front page first.
  airline_field = driver.find_element_by_id('airline-select')
  airline_field.clear()
  carrier_name = flight_attrs['carrier'].split(" ")
  airline_field.send_keys(" ".join(carrier_name))
  airline_autocomplete = driver.find_elements_by_css_selector(".ui-autocomplete.ui-menu.ui-widget.ui-widget-content.ui-corner-all")

  # If the database carrier name has not triggered the autocomplete form,
  # we should shorten the name and try again.
  while not airline_autocomplete.is_displayed() and len(carrier_name) > 1:
    airline.clear()
    carrier_name = carrier_name[0: len(carrier_name) - 1]
    airline.send_keys(" ".join(carrier_name))

  # If we triggered the airline autocomplete, choose the top result.
  # If we could not trigger the autocomplete, we must return False.
  if airline_autocomplete.is_displayed():
    driver.find_element_by_class_name('ui-autocomplete')\
                   .find_element_by_class_name('ui-corner-all').click()
  else:
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
  	  return success

  return success


def get_indices(cursor):
  """
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
  		indices['flight_num']= idx
  	elif tup[1] == "arrival_time":
  		indices['arrival_time'] = idx
  	#elif tup[1] == "date":
  	#	indices['date'] = idx

  # Exit if these indices are not found.
  if None in indices.values():
  	raise Exception('Could not match arrivals table headers. '
  		            'Check header names.')

  # Return the indices dictionary.
  return indices


def extract_plane_insert_and_return(driver, flight_attrs):
  """
  """
  # Boolean for success.
  success = False

  # Grab the top result. Find the airplane type and link.
  airplane = driver.find_element_by_class_name('flightno')
  flight_attrs['aircraft'] = airplane.text

  # Check to see if this entry is already in the database.
  cursor_planes.execute('SELECT * FROM planes '
    					  'WHERE carrier = \'{carrier}\' '
    					    'AND aircraft = \'{aircraft}\';'\
    							        .format(
                                             carrier=flight_attrs['carrier'],
                                             aircraft=flight_attrs['aircraft'])
  
  # If yes, return false.
  if len(cursor_planes.fetchall()) is != 0: return success

  # Some airplane elements are not clickable.  Check and return false if so.
  if not airplane.is_enabled(): return success

  # Navigate to the airplane's page.
  airplane.click()

  # Get all classes corresponding to the seats table.
  seat_list = driver.find_elements_by_class_name('item4')

  # Extract number of seats and assign to plane dictionary.
  num_seats = 0

  for item in seat_list:
    if re.search('[0-9]{1,}', item.text):
	  num_seats += int(re.compile('[0-9]{1,}').search(item.text).group(0))
  
  flight_attrs['total_seats'] = num_seats
  
  # Insert into database.
  cursor_planes.execute(insertion_query.format(
    										carrier=pass,
    										aircraft=pass,
    										total_seats=pass))
  # Return contol.
  driver.execute_script("window.history.go(-1)")
  success = True

  return success


def scrape_planes(driver, database):
  """
  """
  # Open up a connection to the database, initialize a cursor.
  connection = sqlite3.connect(database)
  cursor_arrivals = connection.cursor()
  cursor_planes = connection.cursor()

  # get indices from database.
  indices = get_indices(cursor_arrivals)

  # Point the cursor at the list of flights for which we want plane data.
  cursor.execute('SELECT * FROM arrivals')

  # SOME COUNTERS
  inserted_planes = 0
  bad_planes = 0

  # Iterate through every flight.
  for idx, row in enumerate(cursor):

    # Initiate a dictionary of flight attributes for the web scraping routine.
    flight_attrs = {'carrier': None,
  			 		'flight_num': None,
  			 		'date': None,
  			 		'arrival_time': None,
  			 		'aircraft': None,
  			 		'total_seats': None}

  	# Retrieve the carrier, flight number, arrival_time and date from the sqlite query.
  	flight_attrs['carrier'] = row[indices['airline']]
  	flight_attrs['flight_num'] = row[indices['flight_num']]
  	#flight_attrs['date'] = row[indices['date']]
  	flight_attrs['arrival_time'] = row[indices['arrival_time']]

  	# Fill out the search page.
  	found_potential_plane = fill_search_form_and_submit(driver, flight_attrs)

    # If our search did not turn up a result, we move to the next record.
    if not found_potential_plane: continue

    # If we have a potential plane, let's extract it, insert it into
    # the DB and return control to the search page.
    extracted = extract_plane_insert_and_return(driver, flight_attrs)

    # If we could not extract plane information, move to next record.
    if not extracted: continue

    # Status updates here.
    inserted_planes += 1
    if inserted_planes % 100 == 0:
        print ("Loaded ", inserted_planes,
               " total planes. so far into the database...", sep="")

  # Clean-up resources.
  connection.commit()
  connection.close()


## ====================================================================


def main():
  """
  Main.

  Args:
    None
  
  Returns:
    VOID
  """
  # Load up the webdriver and point it to the top-level URL.
  driver = load_driver(webdriver_exe, url)

  # Create table in SQLite database.
  create_planes_table(customs_db)

  # Initiate Scraping.
  scrape_planes(driver, customs_db)

  return


if __name__ == "__main__": 
  main()

