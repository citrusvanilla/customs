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
A module for optimizing server scheduling for passenger wait time through
simulating throughput of the international arrivals customs.

Performance:

            System             | Analog Time for 24hr sim at 10x Speed
  ---------------------------------------------------------------------
  1 CPU 2.6 GHz Intel Core i5  |              ~4 seconds

Usage:
  Please see the README for how to compile the program and run the
  model.
"""

from __future__ import print_function

import os
import sys
import sqlite3

import pandas as pd

from customs_obj import PlaneDispatcher
from customs_obj import Customs
from customs_obj import _get_sec
from customs_obj import sample_from_triangular


## ====================================================================


# Macros and files.
customs_db = "customs_db.sqlite"
server_schedule_file = "schedules/sample_server_schedule.csv"
report_file = "output/summary.csv"
spd_factor = 10
ave_wait_threshold = 20


## ====================================================================


def simulate(database, plane_dispatcher, server_schedule, speed_factor):
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
  # Initialize a Customs object.
  customs = Customs(database, server_schedule)

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
    #if GLOBAL_TIME % (3600/speed_factor) == 0:
    #  print (GLOBAL_TIME / (3600/speed_factor), " hours: ",
    #         customs.outputs.passengers_served, " passengers serviced.  ", sep='')

  # Write Report Files
  report = customs.generate_report(report_file, customs_db)

  # Clean-up
  customs.clean_up_db()
  del customs

  # Return Pandas dataframe.
  return report


def adjust_schedule(schedule, starting_hour, num_servers):
  """"""

  for hour in range(starting_hour, 24):
    schedule.iloc[0, schedule.columns.get_loc(str(hour))] = num_servers


def init_service_times(database):
  """"""
  # Define service Distributions
  service_dist_dom = ("00:00:30", "00:01:00", "00:02:00")
  service_dist_intl = ("00:01:00", "00:02:00", "00:04:00")

  # Open connection to DB
  connection = sqlite3.connect(database)
  cursor = connection.cursor()

  # Insert a service time attribute.
  cursor.execute('ALTER TABLE passengers ADD service_time INTEGER;')

  # Grab a list of ids.
  ids = cursor.execute('SELECT id FROM passengers;').fetchall()

  # Loop through every passenger and update.
  for passenger_id in ids:
    cursor.execute('UPDATE passengers '
                       'SET service_time = '
                   'CASE WHEN nationality = \'domestic\' '
                       'THEN \'{time_dom}\' '
                       'ELSE \'{time_for}\' END '
                   'WHERE id = \'{id}\';'\
                   .format(time_dom=sample_from_triangular(service_dist_dom), 
                           time_for=sample_from_triangular(service_dist_intl),
                           id=passenger_id[0])) 

  # Commit and close.
  connection.commit()
  connection.close()


def optimize(database, plane_dispatcher, server_schedule, speed_factor, threshold, report_file):
  """"""

  # Define momentum value.
  momentum = 3

  # Adjust schedule to have a max load of servers.
  max_val = server_schedule.iloc[0, server_schedule.columns.get_loc('max')]
  adjust_schedule(server_schedule, 0, max_val)

  # Initialize a pointer to keep track of the previous period.
  previous_hour = None

  # Start the greedy search algorithm by looping through all hours of the sim.
  for hour in range(0, 24):
    
    # Retrieve current number of scheduled servers.
    num_servers = server_schedule.iloc[
                                0, server_schedule.columns.get_loc(str(hour))]

    # Simulate and retrieve sim report.
    data = simulate(database, plane_dispatcher, server_schedule, spd_factor)

    # If there is no activity in the time period, skip forward.
    if hour not in data['hour'].tolist(): continue

    # Retrieve average wait time for the current period.
    ave_wait = int(data[data['hour'] == hour].iloc[0]['ave_wait'])

    # Debug
    print("==========================================================")
    print("Current server schedule:")
    print(data)
    print("==========================================================")

    # Initialize vars for the optimization loop.
    greedy_optimized = False
    new_ave_wait = None

    # Optimization loop.
    while greedy_optimized is False:

      # If the wait time exceeds the threshold, add servers the the simulation.
      # If the wait time falls under the threshold, take servers away.
      if ave_wait >= threshold:
        print ("Average wait in hour ", hour, " for ", num_servers,
             " servers this sim: ", ave_wait, " minutes.", sep="")
        num_servers = num_servers + momentum
        if num_servers > max_val:
          num_servers = max_val
        
        print("Trying ",num_servers, " servers instead.", sep="")

      else:
        print ("Average wait in hour ", hour, " for ", num_servers,
             " servers this sim: ", ave_wait, " minutes.", sep="")
        num_servers = num_servers - momentum
        if num_servers < 1:
          num_servers = 1
        
        print("Trying ",num_servers, " servers instead.", sep="")
        

      # Adjust current and future server counts.
      adjust_schedule(server_schedule, hour, num_servers)

      # Simulate and retrieve average wait time.
      data = simulate(database, plane_dispatcher, server_schedule, spd_factor)
      new_ave_wait = int(data[data['hour'] == hour].iloc[0]['ave_wait'])

      # If our new wait time crosses the threshold the right way, break.
      if ave_wait >= threshold and new_ave_wait < threshold:

        for i in range(momentum - 1):

          print ("Slowing momentum.  Backtracking ", i+1, " servers.", sep="")
          
          # Back track by one server at a time.
          num_servers = num_servers - 1
          
          # Adjust current and future server counts.
          adjust_schedule(server_schedule, hour, num_servers)

          # Simulate and retrieve average wait time.
          data = simulate(database, plane_dispatcher, server_schedule, spd_factor)
          new_ave_wait = int(data[data['hour'] == hour].iloc[0]['ave_wait'])

          if new_ave_wait >= threshold:
            num_servers = num_servers + 1
            adjust_schedule(server_schedule, hour, num_servers)
            break

        greedy_optimized = True

      # If our new wait time crosses the threshold the wrong way,
      # reset server count, and check for breaking conditions in previous period.
      elif ave_wait < threshold and new_ave_wait >= threshold:

        # Back it up for momentum.
        for i in range(momentum):

          print ("Slowing momentum.  Backtracking ", i+1, " servers.", sep="")
          
          # Back track by one server at a time.
          num_servers = num_servers + 1
          
          # Adjust current and future server counts.
          adjust_schedule(server_schedule, hour, num_servers)

          # Simulate and retrieve average wait time.
          data = simulate(database, plane_dispatcher, server_schedule, spd_factor)
          new_ave_wait = int(data[data['hour'] == hour].iloc[0]['ave_wait'])

          if new_ave_wait < threshold:
            break

        # We have to check that the reduction in servers in the current
        # time period still satisfies time restraints for previous periods.
        if previous_hour is not None:
          
          # Look up the previous period's average wait time.
          previous_ave_wait = int(data[data['hour'] == int(previous_hour)].\
                              iloc[0]['ave_wait'])
          
          # If it is greater than the threshold, add servers to current
          # time period and re-evaluate.
          while previous_ave_wait >= threshold and \
                num_servers <= max_val:

            print ("Previous period's optimization violated. "
                   "Adding more servers...")
            num_servers = num_servers + 1
            adjust_schedule(server_schedule, hour, num_servers)
            data = simulate(database, plane_dispatcher, server_schedule,
                            spd_factor)
            previous_ave_wait = int(data[data['hour'] == int(previous_hour)].\
                                iloc[0]['ave_wait'])

        greedy_optimized = True

      # We've hit the max without meeting threshold requirements.
      elif ave_wait >= threshold and num_servers == max_val:
        greedy_optimized = True

      # If our new wait time does not cross a threshold, keep iterating.
      else:
        ave_wait = new_ave_wait
        new_ave_wait = None

    # Hour is optimized. Update pointer to preceeding hour.
    previous_hour = hour

    # Status update for the hour.
    print("==========================================================")
    print ("***Optimized ", num_servers, " servers in time period ", str(hour),
           ".***", sep="")

  # Delete former report files recursively.
  try: os.remove(report_file)
  except OSError: pass

  # Write final report to CSV.
  data = simulate(database, plane_dispatcher, server_schedule, spd_factor)
  data.to_csv(report_file, index=False, columns=["hour", "type", "count",
                                                 "ave_wait", "max_wait",
                                                 "ave_server_utilization",
                                                 "num_servers"])

  # Final Status.
  print(data)
  print("==========================================================")
  print("Optimized model complete.  Written to ", report_file, ".", sep="")
  print("==========================================================")


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

  # Initialize service times for the passengers.
  init_service_times(customs_db)

  # Optimize
  optimize(customs_db, plane_dispatcher, server_schedule, spd_factor,
           ave_wait_threshold, report_file)

  # Clean-up Resources.
  del plane_dispatcher


if __name__ == "__main__":
  main()

