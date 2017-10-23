##
##  JFK Customs Simulation
##  customs.py
##
##  Created by Justin Fung on 10/22/17.
##  Copyright 2017 Justin Fung. All rights reserved.
##
## ====================================================================

"""A module for simulating throughput of the international arrivals
customs at JFK airport.

Performance:


System                       | Simulation Time for 24hours
------------------------------------------------------------
1 CPU 2.6 GHz Intel Core i5  |

Usage:
Please see the README for how to compile the program and run the model.
"""

import pandas as pd
import numpy as np

import customs_obj
import customs_build

from customs_obj import PlaneDispatcher
from customs_obj import Customs
from customs_obj import _get_sec

# Macros.
GLOBAL_TIME = "00:00"
PASSENGER_COUNT = 0


def simulate(arrival_schedule, server_schedule, customs):
  """
  Op:
    Run Customs Simulations for a number of steps.

  Args:
    arrival_schedule: a schedule of plane arrivals as pandas dataframe
    server_schedule: a schedule of online server.
    customs: an instantiated Customs object representing the customs system

  Returns:
    VOID
  """  

  global GLOBAL_TIME

  while GLOBAL_TIME != _get_sec("24:00"):

    # Update the servers according to the server schedule.
    customs.update_servers(server_schedule)

    # Run the plane dispatcher.
    arriving_plane = PlaneDispatcher.dispatch_planes(arrival_schedule)
    
    # Add plane passengers to customs.
    customs.handle_passengers(arriving_plane)

    # Service assignment queues.
    customs.dom_section.assign_passengers()
    customs.intl_section.assign_passengers()

    # Service agent queues.
    customs.dom_section.service_passengers()
    customs.intl_section.service_passengers()

    # Increment global time.
    GLOBAL_TIME += 1



def main(argv=None):
  """
  Main program for running the simulation.  Retrieves schedules, builds the
  representation of the customs system, and simulates the throughput.
  """

  # Retrieve schedules from external csv files.
  arr_schedule = customs_schedules.retrieve_arrivals()
  server_schedule = customs_schedules.retrieve_servers()

  # Build customs graph from external text file.
  customs = customs_build.build()

  # Simulate and output data.
  simulate(arr_schedule, server_schedule, customs)


