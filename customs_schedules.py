##
##  JFK Customs Simulation
##  customs_schedules.py
##
##  Created by Justin Fung on 10/22/17.
##  Copyright 2017 Justin Fung. All rights reserved.
##
## ====================================================================

"""Schedules for simulating throughput of the international arrivals
customs at JFK airport.  Schedules in CSV for arrivals and server
uptime.

Usage:
Please see the README for how to compile the program and run the model.
"""
import pandas as pd
import numpy as np

# Schedule filepaths.
arrivals_csv_file = "schedules/sample_arrivals.csv"
servers_csv_file = "schedules/sample_servers.csv"


def retrieve_arrivals():
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
  arr_sched = pd.read_csv(arrivals_csv_file)
  
  return arr_sched


def retrieve_servers():
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
  servers_sched = pd.read_csv(servers_csv_file)
  
  return servers_sched