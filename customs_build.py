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


def build(server_architecture):
  """
  Builds the Customs network.  Reads architecture into Pandas
  dataframe.  Converts dataframe into Customs object.

  Args:
    server_architecture: a Pandas dataframe

  Returns:
    rtn: an initialized Customs object
  """
  # Convert to Customs obejct.
  rtn = customs_obj.Customs(server_architecture)

  return rtn

