##
##  JFK Customs Simulation
##  customs_build.py
##
##  Created by Justin Fung on 10/22/17.
##  Copyright 2017 Justin Fung. All rights reserved.
##
## ====================================================================

"""Module for building the customs system intended to model throughput
of the JFK international customs.

Usage:
Please see the README for how to compile the program and run the model.
"""
import pandas as pd
import numpy as np

import customs_obj

# Filepath of Customs Architecture
customs_file = "schedules/sample_customs.csv"


def build():
  """
  Builds the Customs network.  Reads architecture into Pandas
  dataframe.  Converts dataframe into Customs object.
  """
  # Import architecture into a Pandas dataframe.
  customs_input = pd.read_csv(customs_file)

  # Convert to Customs obejct.
  rtn = customs_obj.Customs(customs_input)

  return rtn

