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
  1 CPU 2.6 GHz Intel Core i5  |       ~13.5 seconds

Usage:
  Please see the README for how to compile the program and run the
  model.
"""

from __future__ import print_function

import sys

import pandas as pd

from customs_obj import PlaneDispatcher
from customs_obj import Customs
from customs_obj import _get_sec


## ====================================================================


# Macros and files.
customs_db = "customs_db.sqlite"
server_schedule_file = "schedules/sample_server_schedule.csv"
report_file = "passengers_report.csv"
spd_factor = 10


## ====================================================================


def simulate(customs, plane_dispatcher, server_schedule, speed_factor,
             write_output=False):
  """
  Run Customs Simulations for a number of seconds.

  Args:
    customs: an initialized Customs object
    plane_dispatcher: an initialized PlaneDispatcher object
    server_schedule: a Pandas dataframe
    speed_factor: a factor to speed up simulation by only simulating at
                  this time resolution (i.e. every 10 seconds)
    write_output: boolean whether to write output for the passengers
                  and servers.

  Returns:
    VOID
  """

  # Set the global time in seconds, from a string of HH:MM:SS format.
  GLOBAL_TIME = _get_sec("00:00:00", speed_factor)
  END_TIME = _get_sec("24:00:00", speed_factor)

  # Run through the simulation here.
  while GLOBAL_TIME <= END_TIME:

    # Update the online status of the servers.
    customs.update_servers(server_schedule, GLOBAL_TIME)

    # Run the plane dispatcher.
    arriving_planes = plane_dispatcher.dispatch_planes(GLOBAL_TIME)

    # Add plane passengers to customs.
    customs.handle_arrivals(arriving_planes)

    # Assign and service Passengers.
    for section in customs.subsections:

      # Assign Passengers to ServiceAgents.
      section.assignment_agent.assign_passengers()

      # Service the Passengers in the ParallelServers.
      section.parallel_server.service_passengers(GLOBAL_TIME)

    # Write output.
    if write_output is True:
      customs.serviced_passengers.write_out(report_file, GLOBAL_TIME)

    # Increment global time by one unit of time.
    GLOBAL_TIME += 1

    # Provide status update.
    if GLOBAL_TIME % (60/speed_factor) == 0:
      print (GLOBAL_TIME / (60/speed_factor), " mins: ", customs.serviced_passengers.passengers_served,
             " passengers serviced.  ", len(customs.subsections[0].assignment_agent.queue) + \
             len(customs.subsections[1].assignment_agent.queue), " passengers in queue. ",
             (customs.subsections[0].parallel_server.online_server_count + customs.subsections[1].parallel_server.online_server_count),
             " total servers online.", sep='')


## ====================================================================


def main():
  """
  Main program for running the simulation.  Retrieves schedules, builds
  the representation of the customs system, and simulates the
  throughput.
  """

  # Read in the sample server schedule.
  server_schedule = pd.read_csv(server_schedule_file)

  # Build customs graph from the schedule.
  customs = Customs(server_schedule)

  # Initialize a plane dispatcher to handle arrivals from the databse.
  plane_dispatcher = PlaneDispatcher(customs_db)

  # Simulate the throughput, and output the data.
  simulate(customs, plane_dispatcher, server_schedule, spd_factor,
           write_output=True)

  # Clean-up Resources.
  del plane_dispatcher


if __name__ == "__main__":
  main()

