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
report_file = "output/summary.csv"
spd_factor = 10


## ====================================================================


def simulate(customs, plane_dispatcher, server_schedule, speed_factor, write_output=False):
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

      # Capture server utilization.
      section.parallel_server.get_utilization(GLOBAL_TIME)

    # Update passengers
    customs.outputs.update_passengers(customs_db, GLOBAL_TIME)

    # Increment global time by one unit of time.
    GLOBAL_TIME += 1

    # Provide status update.
    if GLOBAL_TIME % (3600/speed_factor) == 0:
      print (GLOBAL_TIME / (3600/speed_factor), " hours: ",
             customs.outputs.passengers_served, " passengers serviced.  ", sep='')

  # Write Report Files
  if write_output is True:
    return customs.generate_report(report_file, customs_db)


def optimize(database, plane_dispatcher, server_schedule, speed_factor, threshold):
  """"""

  # Initialize customs with a full load of servers.
  max_val = server_schedule.iloc[0, server_schedule.columns.get_loc('max')]
  for hour in server_schedule.columns[2:]:
    server_schedule.iloc[0, server_schedule.columns.get_loc(hour)] = max_val
  customs = Customs(customs_db, server_schedule)




  # Start the greedy search algorithm.
  for hour in server_schedule.columns[2:]:
    
    # Current number of servers.
    num_servers = server_schedule.iloc[0, server_schedule.columns.get_loc(hour)]

    # Simulate to get the wait times.
    data = simulate(customs, plane_dispatcher, server_schedule, spd_factor, write_output=True)
    print(data)

    if int(hour) not in data['hour'].tolist():
      continue

    ave_wait = int(data[data['hour'] == int(hour)].iloc[0]['ave_wait'])
    print (ave_wait >= threshold)
    print (ave_wait, threshold)

    greedy_optimized = False
    new_ave_wait = None

    while greedy_optimized is False:

      # If the wait time exceeds the threshold, add servers and try again.
      if ave_wait >= threshold:
        num_servers = num_servers + 1

      # If the wait time falls under the threshold, take servers away and try again.
      else:
        num_servers = num_servers - 1

      for hour2 in range(int(hour),24):
        if int(hour2) >= int(hour):
          server_schedule.iloc[0, server_schedule.columns.get_loc(str(hour2))] = num_servers

      # Debug
      print ("Average wait for this sim: ", ave_wait, ". Trying ", num_servers, " servers.", sep="")

      # reint customs
      customs.clean_up_db()
      del customs
      customs = Customs(customs_db, server_schedule)

      # Simulate to get the wait times.
      data = simulate(customs, plane_dispatcher, server_schedule, spd_factor, write_output=True)
      new_ave_wait = int(data[data['hour'] == int(hour)].iloc[0]['ave_wait'])





      # If our new wait time crosses the threshold the right way, break.
      if ave_wait >= threshold and new_ave_wait < threshold:
        greedy_optimized = True

      # If our new wait time crosses the threshold the wrong way,
      # reset server config and break.
      elif ave_wait < threshold and new_ave_wait >= threshold:
        num_servers = num_servers + 1
        for hour2 in range(int(hour),24):
          if int(hour2) >= int(hour):
            server_schedule.iloc[0, server_schedule.columns.get_loc(str(hour2))] = num_servers
        greedy_optimized = True

      # If our new wait time does not cross a threshold, keep trying.
      else:
        ave_wait = new_ave_wait
        new_ave_wait = None




    print ("Optimized ", num_servers, " servers in time period ", str(hour), ".", sep="")

  # clean-up
  customs.clean_up_db()
  del customs


## ====================================================================


def main():
  """
  Main program for running the simulation.  Retrieves schedules, builds
  the representation of the customs system, and simulates the
  throughput.
  """

  # Read in the sample server schedule.
  server_schedule = pd.read_csv(server_schedule_file)

  # Initialize a plane dispatcher to generate arrivals from the databse.
  plane_dispatcher = PlaneDispatcher(customs_db)

  # Optimize
  optimize(customs_db, plane_dispatcher, server_schedule, spd_factor, 30)




  # Build customs graph from the schedule.
  #customs = Customs(customs_db, server_schedule)

  # Simulate the throughput, and output the data.
  #simulate(customs, plane_dispatcher, server_schedule, spd_factor, write_output=True)

  # Clean-up Resources.
  #customs.clean_up_db()
  del plane_dispatcher#, customs


if __name__ == "__main__":
  main()

