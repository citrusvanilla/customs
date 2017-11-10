##
##  JFK Customs Simulation
##  customs.py
##
##  Created by Justin Fung on 10/22/17.
##  Copyright 2017 Justin Fung. All rights reserved.
##
## ====================================================================
# pylint: disable=bad-indentation,bad-continuation,multiple-statements
# pylint: disable=invalid-name,trailing-newlines
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

from __future__ import print_function

import sys

import pandas as pd

from customs_obj import PlaneDispatcher
from customs_obj import Customs
from customs_obj import _get_sec


## ====================================================================


# Macros.
customs_db = "customs_db.sqlite"
server_schedule_file = "schedules/sample_server_schedule.csv"


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
  GLOBAL_TIME = _get_sec("00:00:00")

  while GLOBAL_TIME <= _get_sec("24:00:00"):

    # Update the servers according to the server schedule.
    customs.update_servers(server_schedule, GLOBAL_TIME)

    # Run the plane dispatcher.
    arriving_planes = plane_dispatcher.dispatch_planes(GLOBAL_TIME)

    # Add plane passengers to customs.
    customs.handle_arrivals(arriving_planes)

    # Assign and Service Passengers.
    for section in customs.subsections:

      # Assign Passengers to ServiceAgents.
      section.assignment_agent.assign_passengers()

      # Service Passengers in the ParallelServers.
      section.parallel_server.service_passengers(GLOBAL_TIME)

    # Increment global time.
    GLOBAL_TIME += 1

    # Provide status update.
    if GLOBAL_TIME % 60 == 0:
      print (GLOBAL_TIME / 60, " mins: ", len(customs.serviced_passengers.queue),
             " passengers serviced.", sep="")
      '''print (len(customs.subsections[0].assignment_agent.queue))
      print (len(customs.subsections[1].assignment_agent.queue))
      print (customs.subsections[0].parallel_server.has_space_in_a_server_queue)
      print (customs.subsections[1].parallel_server.has_space_in_a_server_queue)'''


## ====================================================================


def main():
  """
  Main program for running the simulation.  Retrieves schedules, builds
  the representation of the customs system, and simulates the
  throughput.
  """
  # Command line arguments.
  server_schedule = pd.read_csv(server_schedule_file)
  #start_time, end_time = sys.argv[2], sys.argv[3]
  #maxnum_servers_dom, maxnum_servers_intl = sys.argv[3], sys.argv[4]

  # Build customs graph from server CSV file.
  customs = Customs(server_schedule)

  # Initialize a plane dispatcher to handle arrivals.
  plane_dispatcher = PlaneDispatcher(customs_db)

  # Simulate and output data.
  simulate(customs, plane_dispatcher, server_schedule)


if __name__ == "__main__":
  main()

