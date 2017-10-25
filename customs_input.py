##
##  JFK Customs Simulation
##  customs_input.py
##
##  Created by Justin Fung on 10/22/17.
##  Copyright 2017 Justin Fung. All rights reserved.
##
## ====================================================================

"""Handles schedules and server architecture for simulating throughput
of the international arrivals customs at JFK airport.  Schedules in CSV
for arrivals and server uptime.

Usage:
Please see the README for how to compile the program and run the model
and CSV formatting requirements.
"""
import pandas as pd
import numpy as np

# Schedule filepaths.
arrival_schedule_csvfile = "schedules/sample_arrival_schedule.csv"
server_schedule_csvfile = "schedules/sample_server_schedule.csv"

# Customs Architecture filepath.
customs_arch_csvfile = "schedules/sample_arrival_schedule.csv"


## ====================================================================


def retrieve_arrival_schedule():
  """
  Function for retrieving a CSV of plane arrivals, sorted by arrival
  time.  Pandas library converts CSV into dataframe.  For correct CSV
  formatting, see the README.

  Args:
	NONE

  Returns:
    arr_sched: Pandas Dataframe.
  """
  # Import with Pandas.
  arr_sched = pd.read_csv(arrival_schedule_csvfile)
  
  return arr_sched


def retrieve_server_schedule():
  """
  Function for retrieving a CSV of server uptimes. Pandas library
  converts CSV into dataframe.  For correct CSV formatting, see the
  README.

  Args:
    NONE
  
  Returns:
    servers_sched: Pandas Dataframe.
  """
  # Import with Pandas.
  servers_sched = pd.read_csv(server_schedule_csvfile)
  
  return servers_sched


def retrieve_server_architecture():
  """
  Function for retrieving a CSV of server architecture. Pandas library
  converts CSV into dataframe.  For correct CSV formatting, see the
  README.

  Args:
    NONE
  
  Returns:
    servers_arch: Pandas Dataframe.
  """
  # Import with Pandas.
  servers_arch = pd.read_csv(customs_arch_csvfile)
  
  return servers_arch