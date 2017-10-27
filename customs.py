##
##  JFK Customs Simulation
##  customs.py
##
##  Created by Justin Fung on 10/22/17.
##  Copyright 2017 Justin Fung. All rights reserved.
##
## ====================================================================
"""
A module for simulating throughput of the international arrivals
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
GLOBAL_TIME = _get_sec("00:00:00")
PASSENGER_COUNT = 0

## ====================================================================


def simulate(customs, plane_dispatcher, server_schedule):
  """
  Run Customs Simulations for a number of steps.

  Args:
    customs: an initialized Customs object
    plane_dispatcher: an initialized PlaneDispatcher object
    server_schedule: a Pandas dataframe

  Returns:
    VOID
  """  
  global GLOBAL_TIME

  while GLOBAL_TIME <= _get_sec("24:00:00"):

    # Update the servers according to the server schedule.
    customs.update_servers(server_schedule)

    # Run the plane dispatcher.
    arriving_plane = plane_dispatcher.dispatch_plane(GLOBAL_TIME)
    
    # Add plane passengers to customs.
    customs.handle_passengers(arriving_plane)

    # Assign and Service Passengers.
    for section in customs.subsections:
        # Assign Passengers to ServiceAgents.
        section.assignment_agent.assign_passengers(section.parallel_server)
        # Service Passengers in the ParallelServers.
        section.parallel_server.service_passengers()

    # Increment global time.
    GLOBAL_TIME += 1


def main(argv=None):
  """
  Main program for running the simulation.  Retrieves schedules, builds the
  representation of the customs system, and simulates the throughput.
  """

  # Retrieve schedules and architecture from external CSV files.
  arrival_schedule = customs_input.retrieve_arrival_schedule()
  server_schedule = customs_input.retrieve_server_schedule()
  server_architecture = customs_input.retrieve_server_architecture()

  # Build customs graph from external text file.
  customs = customs_build.build(server_architecture)

  # Initialize a plane dispatcher to handle arrivals.
  plane_dispatcher = PlaneDispatcher(arrival_schedule)

  # Simulate and output data.
  simulate(customs, plane_dispatcher, server_schedule)

