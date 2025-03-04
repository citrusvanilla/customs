##
##  JFK Customs Simulation
##  customs_obj.py
##
##  Created by Justin Fung on 10/22/17.
##  Copyright 2017 Justin Fung. All rights reserved.
##
## ====================================================================
# pylint: disable=bad-indentation,bad-continuation,multiple-statements
# pylint: disable=invalid-name,trailing-newlines

"""
Objects for simulating throughput of the international arrivals
customs at JFK airport.  The customs system is modeled through OO
construction.

Usage:
  Please see the README for how to compile the program and run th
  model.
"""

from __future__ import print_function

from collections import deque

import csv
import re
import sqlite3
import numpy as np
import pandas as pd
import os
import time


## ====================================================================


# Random seed
np.random.seed(int(time.time()))

# Hourly timestamps (for output)
hourly_timestamps = ["0" + str(i) + ":00:00" for i in range(0,10)] + \
                    [str(i) + ":00:00" for i in range(10,24)]

# Service Distributions
service_dist_dom = ("00:00:30", "00:01:00", "00:02:00")
service_dist_intl = ("00:01:00", "00:02:00", "00:04:00")

# Speed up factor
spd_factor = 10

# Helper functions
def _get_sec(time_str, speed_factor):
  """
  Convert a string in "HH:MM:SS" format to seconds as an integer,
  adjusted by a time resolution ("speed") factor.

  Args:
    time_str: a string in "HH:MM:SS" format
    speed_factor: integer for factor for time resolution

  Returns:
    seconds: an integer
  """

  # Split string and convert.
  h, m, s = time_str.split(':')
  seconds = int(h) * 3600 + int(m) * 60 + int(s)

  # Adjust for speed.
  seconds = seconds / speed_factor

  return seconds


def _get_ttime(seconds, speed_factor):
  """
  Convert seconds as an integer to "HH:MM:SS" format, adjusted for a
  time resolution ("speed") factor.

  Args:
    seconds: an integer
    speed_factor: an iteger for time resolution

  Returns:
    time_str: a string in "HH:MM:SS" format
  """

  # Adjust for speed.
  seconds = seconds * speed_factor

  # Factor out hours, minutes and seconds into integers.
  h = int(seconds/3600)
  m = int(seconds%3600)/60
  s = int(seconds%3600)%60

  # If number of characters of the factors is 1, prepend zeroes to string.
  HH = "0" + str(h) if int(h/10) == 0 else str(h)
  MM = "0" + str(m) if int(m/10) == 0 else str(m)
  SS = "0" + str(s) if int(s/10) == 0 else str(s)

  # Concatenate result and return.
  time_str = HH + ":" + MM + ":" + SS
  return time_str


def sample_from_triangular(service_dist):
  """
  Returns number of seconds.

  Args:
    service_dist: tuple of lower, middle, upper parameters of a
                  triangular distribution as strings in MM:SS format.

  Returns:
    sample: service time in seconds as integer

  """

  # Convert from strings.
  lower = _get_sec(service_dist[0], spd_factor)
  mode = _get_sec(service_dist[1], spd_factor)
  upper = _get_sec(service_dist[2], spd_factor)

  # Sample using Numpy triangular, and return.
  sample = int(np.random.triangular(lower, mode, upper))
  return sample


## ====================================================================


class PlaneDispatcher(object):
  """
  Hanlder Class for managing arrivals schedule and building planes and
  their passengers in accordance with the schedule.  Should be
  instantiated only one per simulation.

  Member Data:
    connection: initialized connection to sqlite database
    cursor: initialized cursor for querying sqlite database
    intl_arrival_dict: dictionary with arrival times as keys and plane
                       ids as values
    intl_arrival_times: a set of unique arrival times of international
                        arrivals
    plane_count: simple integer count of planes initialized
    passenger_count: simple integer count of passengers initialized

  Member Functions:
    dispatch_plane: returns initialized planes if simulation time
                    matches an arrival.
    get_intl_arrivals: gets a dict of international arrival times and
                       plane ids from a sqlite database
    __del__: overwritten destroyer function to close sqlite connection
  """

  def __init__(self, sqlite_database):
    """
    PlaneDispatcher must be instantiated with an arrivals schedule
    whose format is specified in the README.
    """
    self.connection = sqlite3.connect(sqlite_database)
    self.cursor = self.connection.cursor()
    self.intl_arrival_dict = self.get_intl_arrivals()
    self.intl_arrival_times = set(self.intl_arrival_dict.keys())
    self.plane_count = 0
    self.passenger_count = 0


  def get_intl_arrivals(self):
    """
    Returns a dict of unique international arrivals stored as time/id
    values.

    Args:
      None

    Returns:
      dic: a python dictionary
    """

    # Fetch international arrival times from database.
    arrival_id_and_times = self.cursor.execute(
                                'SELECT arrivals.arrival_time, arrivals.id '
                                'FROM arrivals LEFT JOIN airports '
                                  'ON arrivals.airport_code = airports.code '
                                'WHERE arrivals.code_share = \'\' '
                                  'AND arrivals.terminal = \'4\' '
                                  'AND airports.country != \"United States\" '
                                  'AND airports.preclearance != \"true\";')\
                                .fetchall()

    # Dict the results and return.
    dic = {}
    for arrival in arrival_id_and_times:
      if arrival[0] in dic:
        dic[arrival[0]].append(arrival[1])
      else:
        dic[arrival[0]] = [arrival[1]]
    return dic


  def dispatch_planes(self, current_time):
    """
    PlaneDispatcher class method for initializing and returning a new
    plane on schedule.

    Args:
      current_time: simulation time in simulation time units.

    Returns:
      planes: a list of instantiated Plane objects
    """

    # Init a list to hold the plane objects.
    planes = []

    # If a plane is not due, return empty list immediately.
    if _get_ttime(current_time, spd_factor) not in self.intl_arrival_times:
      return planes

    # Query dictionary for international arrival plane ids.
    planes_list = self.intl_arrival_dict[_get_ttime(current_time, spd_factor)]

    # Build SQL statement.
    sql = ('SELECT id, origin, airport_code, arrival_time, airline, '
              'flight_num, terminal '
            'FROM arrivals '
            'WHERE id in (%s);')
    in_p = ', '.join(map(lambda x: '?', planes_list))
    sql = sql % in_p

    # Execute SQL statement to obtain arrivals.
    arrivals = self.cursor.execute(sql, planes_list).fetchall()

    # For each arrival, init a new Plane object.
    for arrival in arrivals:

      # Grab arrival attributes.
      pid, origin, airport_code, \
      arrival_time, airline, flight_num, terminal = arrival

      # Grab the passenger manifest from the database.
      plist = self.cursor.execute('SELECT id, '
                                     'flight_num, '
                                     'first_name, '
                                     'last_name, '
                                     'birthdate, '
                                     'nationality, '
                                     'service_time '
                                  'FROM passengers '
                                  'WHERE flight_num = \'{flight_num}\';'\
                                   .format(flight_num=flight_num)).fetchall()

      # Init a Plane object and append to list.
      planes.append(Plane(pid,
                          origin,
                          airport_code,
                          arrival_time,
                          airline,
                          flight_num,
                          terminal,
                          plist))

      # Increment counts for planes and passengers dispatched.
      self.plane_count += 1
      self.passenger_count += len(plist)

    # Return the Plane objects, if any.
    return planes


  def __del__(self):
    '''
    Overwrites default destroyer method to close the sqlite database
    connection.

    Args:
      None

    Returns:
      VOID
    '''

    # Close the connection.
    self.connection.close()


class Plane(object):
  """
  Class representing an arriving Plane.

  Member Data:
    id: ID of plane as a string
    origin: origin of plane as a string
    airport_code: origin airport code or plane as a string
    arrival_time: arrival time of the plane as a string in HH:MM:SS
                  format
    airline: airline carrier of the plane as string
    flight_num: flight number of the plane as a string
    terminal: arrival terminal of the plane as a string
    plist: passenger manifest of the plane as a python list

  Member Fuctions:
    init_plist: initializes a list of passengers upon instantiation
                of the Plane object
  """

  def __init__(self, plane_id, origin, airport_code, arrival_time, airline,
               flight_num, terminal, passenger_list):
    """
    Plane class initializer method.
    """
    self.id = plane_id
    self.origin = origin
    self.airport_code = airport_code
    self.arrival_time = arrival_time
    self.airline = airline
    self.flight_num = flight_num
    self.terminal = terminal
    self.num_dom_passengers = 0
    self.num_intl_passengers = 0
    self.plist = self.init_plist(passenger_list)


  def init_plist(self, passenger_list):
    """
    Plane class member function for initializing a list of passenger
    objects.

    Args:
      passenger_list: a list of passengers and their attributes
                      represented as tuples

    Returns:
      plist: a list of initialized Passenger objects.
    """

    # Init empty list to hold Passenger objects.
    plist = []

    # Iterate through the flight manifest.
    for passenger in passenger_list:

      # Grab attributes.
      pid, flight_num, first_name, \
      last_name, birthdate, nationality, service_time = passenger

      # Init and append Passenger object with attributes.
      plist.append(Passenger(pid,
                             flight_num,
                             self.arrival_time,
                             first_name,
                             last_name,
                             birthdate,
                             nationality,
                             service_time))

      # Increment passenger count.
      if plist[-1].nationality == 'domestic':
        self.num_dom_passengers += 1
      else:
        self.num_intl_passengers += 1

    # Return list of instantiated Passenger objects.
    return plist


class Passenger(object):
  """
  Class to represent a passenger.

  Member Data:
    id: numeric id of passenger as string
    flight_num: flight number of the passenger as string
    arrival_time: arrival time of the passenger as string in HH:MM:SS
    first_name: first name of the passenger as string
    last_name: last name of the passenger as string
    birthdate: birthdate of the passenger as string
    nationality = foreign/domestic designation
    enque_time = time of arrival to customs
    departure_time = time of departure
    service_time = time of service by service agent
    conntect_flight = boolean for having a connecting conntect_flight
    processed = whether or not the passenger has been serviced_passengers

  Member Functions:
    init_service_time: update function to indicate that service has begun.
    __iter__: overwritten iteration behavior for passenger class
  """

  def __init__(self, pid, flight_num, arrival_time, first_name, last_name,
               birthdate, nationality, service_time):
    '''
    Passenger class must be initialized with nationality and global time.
    '''
    self.id = pid
    self.flight_num = flight_num
    self.arrival_time = arrival_time
    self.first_name = first_name
    self.last_name = last_name
    self.birthdate = birthdate
    self.nationality = nationality
    self.enque_time = _get_sec(arrival_time, spd_factor)
    self.departure_time = -1
    self.service_time = int(service_time)
    self.connecting_flight = False
    self.processed = False


  def init_service_time(self):
    """
    Generate a random service time from a passed distribution.

    Args:
      None

    Returns:
      VOID
    """

    if self.nationality == "domestic":
      return sample_from_triangular(service_dist_dom)
    elif self.nationality == "foreign":
      return sample_from_triangular(service_dist_intl)


  def __iter__(self):
    """
    Define iterable behavior of the Passenger class.
    """

    return iter([self.id, self.id, self.flight_num, self.arrival_time,
                 self.first_name, self.last_name, self.birthdate,
                 self.nationality, self.enque_time, self.departure_time,
                 self.service_time, self.connecting_flight, self.processed])


## ====================================================================


class Customs(object):
  """
  Wrapper class representing the Customs system.

  Member Data:
    serviced_passengers: a class that holds all processed passengers
    subsections: a list of subsections containing parallel servers and
                 assignment agents/servers

  Member Functions:
    init_subsections: initializes list of Subsections objects
    handle_arrivals: accepts Plane objects and fills queues with Passengers
    update_servers: updates individual servers online/offline statuses
  """

  def __init__(self, database, server_architecture):
    """
    Customs Class initialization member function.
    """
        # connection
    self.connection = sqlite3.connect(database)
    self.cursor = self.connection.cursor()
    self.outputs = Outputs()
    self.subsections = self.init_subsections(server_architecture)
    self.prep_database(database)


  def prep_database(self, database):
    """"""
    self.cursor.execute('ALTER TABLE passengers ADD enque_time INTEGER;')
    self.cursor.execute('ALTER TABLE passengers ADD departure_time INTEGER;') 
    self.cursor.execute('ALTER TABLE passengers ADD connecting_flight bool;')
    self.cursor.execute('ALTER TABLE passengers ADD processed bool;')
    self.connection.commit()


  def init_subsections(self, customs_arch):
    """
    Customs Class member function for initializing Subsection objects.

    Args:
      customs_arch: a pandas dataframe representing the architecture of
                    the servers

    Returns:
      section_list: a list of initialized Subsection objects
    """
    section_list = []

    # Identify number of unique subsections in the server arcitecture.
    num_subsections = len(customs_arch['subsection'].unique())

    # Retrieve number of total servers and start an ID counter.
    num_servers = sum(customs_arch['max'])
    server_id = 1

    # Initialize each Subsection Class with a loop.
    for i in range(num_subsections):

      # Get the label of the subsection.
      subsection_id = customs_arch['subsection'].unique()[i]

      # Subset the master architecture into an architecture just for
      # for the subsection.
      subsection_arch = customs_arch[customs_arch['subsection'] == subsection_id]

      # Get server ID range.
      server_range = (server_id, server_id + subsection_arch.iloc[0]['max'])
      server_id = server_id + subsection_arch.iloc[0]['max']

      # Get the processed passenger queue from the Class Data Members list.
      serviced_passengers_list = self.outputs

      # Init a subsection and append to the list.
      section_list.append(Subsection(subsection_id,
                                     subsection_arch,
                                     server_range,
                                     serviced_passengers_list))

    return section_list


  def handle_arrivals(self, planes):
    """
    Method for handling a list of planes of arriving passengers.

    Args:
      planes: a list of initialized Plane objects

    Returns:
      VOID
    """
    # Immediately return if there are no planes to handle.
    if not planes: return

    # Get the subsection indices for dom/intl queues.
    #id_domestic = 0 if self.subsections[0].id == 'domestic' else 1
    #id_foreign = 0 if self.subsections[0].id == 'foreign' else 1

    # Loop through the list of Planes.
    for plane in planes:

      # While the plane still has passengers on it...
      while len(plane.plist) > 0:

        passenger = plane.plist.pop()

        for subsection in self.subsections:
  
          # If national, move to national queue.
          if passenger.nationality == subsection.id :
            subsection.assignment_agent.queue.append(passenger)
            break
            

          # If intl, move to intl queue.
          #elif plane.plist[-1].nationality == "foreign":
          #  self.subsections[id_foreign].assignment_agent.queue.append(
          #                                                    plane.plist.pop())


  def update_servers(self, server_schedule, current_time):
    """
    Updates online/offline status of servers in parallel.

    Args:
      server_schedule: a Pandas dataframe
      current_time: simulation time in simulation time units

    Returns:
      VOID
    """

    # If we are not on an hour, we skip.
    if current_time % _get_sec("01:00:00", spd_factor) != 0: return

    # If we at the end of the simulation, skip.
    if current_time == _get_sec("24:00:00", spd_factor): return

    # Use the global time to identify the apposite column of the schedule.
    time_idx = None

    # Loop through the server schedule columns to find the correct column.
    for idx, i in enumerate(server_schedule.columns):

      # Ensures the column names of the server schedule are formatted as:
      # "0", "1", ... "23"...
      if re.search('[0-9]+', i):

        # Finds the column in the server schedule that corresponds
        # with the global time and stores that index.
        if _get_sec(i.strip()+ ":00:00", spd_factor) <= current_time < \
           _get_sec(str(int(i.strip())+1)+ ":00:00", spd_factor) :
          time_idx = idx
          break

    # Loop through all subsections.parallel_server.server_list:
    for section in self.subsections:

      # Retrieve the section schedule from the master schedule.
      schedule = server_schedule[server_schedule['subsection'] == section.id]

      # Retrieve number of online servers for the given time.
      num_servers = schedule.iloc[0][time_idx]

      # Loop through every server in the server list.
      for counter, server in enumerate(section.parallel_server.server_list):

        if counter < num_servers:
          server.online = True
        else:
          server.online = False


  def generate_report(self, output_file, database):
    """"""

    # Init an empty dataframe to hold the server utilization Series.
    server_df = pd.DataFrame()

    # Loop through all the sections in the subsections.
    for section in self.subsections:

      # Loop through all the servers in the secions.
      for server in section.parallel_server.server_list:

        # Add the server type to the Pd series.
        server.utilization_series.set_value('type', server.type)

        # Concat the server's utilization series to the dataframe.
        server_df = pd.concat([server_df, server.utilization_series], axis=1)

    # Insert into database
    server_df = server_df.transpose()
    server_df.to_sql('servers', self.connection, if_exists='replace')

    # Perform a summary queries.
    server_data = pd.read_sql(
              'SELECT type, avg("00:00:00") as \'0\', avg("01:00:00") as \'1\', '
                 'avg("02:00:00") as \'2\', avg("03:00:00") as \'3\', '
                 'avg("04:00:00") as \'4\', avg("05:00:00") as \'5\', '
                 'avg("06:00:00") as \'6\', avg("07:00:00") as \'7\', '
                 'avg("08:00:00") as \'8\', avg("09:00:00") as \'9\', '
                 'avg("10:00:00") as \'10\', avg("11:00:00") as \'11\', '
                 'avg("12:00:00") as \'12\', avg("13:00:00") as \'13\', '
                 'avg("14:00:00") as \'14\', avg("15:00:00") as \'15\', '
                 'avg("16:00:00") as \'16\', avg("17:00:00") as \'17\', '
                 'avg("18:00:00") as \'18\', avg("19:00:00") as \'19\', '
                 'avg("20:00:00") as \'20\', avg("21:00:00") as \'21\', '
                 'avg("22:00:00") as \'22\', avg("23:00:00") as \'23\' '
              'FROM servers group by type;', self.connection)

    server_counts = pd.read_sql(
            'SELECT type, '
               'count("00:00:00") as \'0\', count("01:00:00") as \'1\', '
               'count("02:00:00") as \'2\', count("03:00:00") as \'3\', '
               'count("04:00:00") as \'4\', count("05:00:00") as \'5\', '
               'count("06:00:00") as \'6\', count("07:00:00") as \'7\', '
               'count("08:00:00") as \'8\', count("09:00:00") as \'9\', '
               'count("10:00:00") as \'10\', count("11:00:00") as \'11\', '
               'count("12:00:00") as \'12\', count("13:00:00") as \'13\', '
               'count("14:00:00") as \'14\', count("15:00:00") as \'15\', '
               'count("16:00:00") as \'16\', count("17:00:00") as \'17\', '
               'count("18:00:00") as \'18\', count("19:00:00") as \'19\', '
               'count("20:00:00") as \'20\', count("21:00:00") as \'21\', '
               'count("22:00:00") as \'22\', count("23:00:00") as \'23\' '
            'FROM servers group by type;', self.connection)
    
    passenger_data = self.cursor.execute(
              'SELECT arrival_hour, '
                'nationality, '
                'count(*) as count, '
                'avg(wait_time) as wait_time, '
                'max(wait_time) as max_wait '
              'FROM '
                '(SELECT cast(enque_time/\'{hour}\' as int) as arrival_hour, '
                   'departure_time - enque_time as wait_time, '
                   'nationality '
                 'FROM passengers '
                 'WHERE enque_time is NOT NULL) '
              'GROUP BY 1, 2;'.format(
                    hour=_get_sec("01:00:00", spd_factor))).fetchall()

    # Headers
    headers = ["hour", "type", "count", "ave_wait", "max_wait",
               "ave_server_utilization", "num_servers"]
    idx_1 = 0
    idx_2 = 0
    output_df = pd.DataFrame()

    for row in passenger_data:
      row = list(row)
      while str(server_data.columns[idx_1]) != str(row[0]): idx_1 += 1
      while str(server_counts.columns[idx_2]) != str(row[0]): idx_2 += 1

      output_row = pd.Series(
         (row[0:3] + \
         [int(float(row[3]) / _get_sec("1:00:00",spd_factor) * 60)] + \
         [int(float(row[4]) / _get_sec("1:00:00",spd_factor) * 60)] + \
         [round(server_data[server_data['type'] == str(row[1])].iloc[0][idx_1],2)] + \
         [server_counts[server_counts['type'] == str(row[1])].iloc[0][idx_2]]),
         index=headers)

      output_df = pd.concat([output_df, output_row], axis=1)

    output_df = output_df.transpose()

    # Clean up
    self.connection.commit()

    return output_df


  def clean_up_db(self):
    self.connection.execute('DROP TABLE IF EXISTS servers;')

    self.connection.execute('ALTER TABLE passengers RENAME TO tmp_passengers;')

    self.connection.execute('CREATE TABLE passengers ('
                                   'id integer PRIMARY KEY, '
                                   'flight_num text, '
                                   'first_name text, '
                                   'last_name text, '
                                   'birthdate text, '
                                   'nationality text, '
                                   'service_time);')

    self.connection.execute('INSERT INTO passengers '
                              'SELECT id, '
                                 'flight_num, '
                                 'first_name, '
                                 'last_name, '
                                 'birthdate, '
                                 'nationality, '
                                 'service_time '
                              'FROM tmp_passengers;')

    self.connection.execute('DROP TABLE tmp_passengers;')

    self.connection.commit()


  def __del__(self):
    """"""
    self.connection.close()


class Subsection(object):
  """
  Class representing subqueues of a Customs system.  Traditionally, a
  Customs system would consist of two or three subsections of which one
  is a domestic processing queue for nationals and the other is an
  international processing queue for aliens.

  Member Data:
    id: a string
    assignment_agent: an initialized AssignmentAgent object
    parallel_server: an initialized ParallelServer object
  """
  def __init__(self, subsection_id, subsection_arch, server_range,
               serviced_passengers):
    """
    Subsection Class initialization function.

    Args:
      subsection_arch: a Pandas dataframe
      serviced_passengers: a python list
    """
    self.id = subsection_id
    self.parallel_server = ParallelServer(subsection_arch,
                                          subsection_id,
                                          server_range,
                                          serviced_passengers)
    self.assignment_agent = AssignmentAgent(self.parallel_server)


class ParallelServer(object):
  """
  Class representing a block of servers in parallel.

  Member Data:
    server_list: a list of initialized ServiceAgent objects.
    has_space_in_a_server_queue: boolean indicating if an agent is not
                                 has a non-full queue.
    queues_size: total number of Passengers waiting in queues.
    min_queue: a ServiceAgent object containing shortest queue length.

  Member Function:
    init_server_list: initializes a list of ServiceAgent objects
    service_passengers: moves Passengers from queues to booths
    update_min_queue: updates the identity of the smallest queue
    update_has_space_in_a_server_queue:
  """

  def __init__(self, subsection_arch, subsection_id, server_range,
               serviced_passengers):
    """
    ParallelServer Class initialization member function.

    Args:
      subsection_arch: a Pandas dataframe
      serviced_passengers: a python list
    """
    self.server_list = self.init_server_list(subsection_arch,
                                             subsection_id,
                                             server_range,
                                             serviced_passengers)
    self.has_space_in_a_server_queue = True
    self.queue_size = 0
    self.min_queue = self.server_list[0]
    self.online_server_count = 0


  def init_server_list(self, subsection_arch, subsection_id, server_range,
                       output_list):
    """
    ParallelServer Class member function that initializes a list of
    ServiceAgent objects.

    Args:
      subsection_arch: a Pandas dataframe.
      output_list: a python list.

    Returns:
      rtn: a list of initialized ServiceAgent objects.
    """

    # Init a list of servers to return.
    rtn = []

    # Loop through all servers in the arch.
    for i in range(server_range[0], server_range[1]):

      # Pass the ID of the server and Init a server.
      rtn.append(ServiceAgent(str(i), subsection_id, output_list))

    # Return the list.
    return rtn


  def service_passengers(self, current_time):
    """
    ParallelServer Class member function that services Passenger
    objects by moving them from queues to booths.

    Args:
      None

    Returns:
      VOID

    """
    # Loop through all the servers.
    for server in self.server_list:
      server.serve(current_time)


  def update_state(self):
    """
    ParallelServer Class member function that updates the statuses of
    the servers in the Parallel server block.

    Args:
      None

    Returns:
      VOID
    """

    # Start off assuming no space in the queues and no pointer to a
    # shortest queue.
    self.min_queue = None
    self.has_space_in_a_server_queue = False
    self.queue_size = 0
    self.online_server_count = 0

    # Loop through all the servers.
    for server in self.server_list:

      # If server is online....
      if server.online is True:

        # Increment count of online servers
        self.online_server_count += 1

        # If any server has space...
        if len(server.queue) < server.max_queue_size:

          # 'Has Space' is True and remains true.
          if self.has_space_in_a_server_queue is False:
            self.has_space_in_a_server_queue = True

          # First non-full server we come to.
          if self.min_queue is None:
            self.min_queue = server

          # If we already had a non-full queue in hand,
          # compare it to the present one.
          elif len(server.queue) < len(self.min_queue.queue):
            self.min_queue = server

        # Increment the count of the parallel server block.
        self.queue_size += len(server.queue)


  def get_utilization(self, current_time):
    """
    Method for getting utilization.

    Args:
      current_time: simulation time in sim time units

    Returns:
      VOID
    """

    # Calculate utilization for all servers.
    for server in self.server_list:
      server.get_utilization(current_time)


class AssignmentAgent(object):
  """
  Class for representing a "bottleneck" agent in charge of assigning
  Passengers in a single queue to a multi-queue server.

  Member Data:
    queue: a list of Passengers sorted by arrival time
    current_passenger: pointer to a Passenger at head of queue

  Member Functions:
    assign_passengers: moves a Passenger from self.queue to a server.queue
  """
  def __init__(self, parallel_server):
    """
    AssignmentAgent Class initialization Member Function.
    """
    self.queue = deque()
    self.parallel_server = parallel_server


  def assign_passengers(self):
    """
    AssignmentAgent Class member function that moves a passenger to a
    Service Agent conditional on a set of requirements.

    Args:
      parallel_server_obj: a Passenger object

    Returns:
      VOID: moves a Passenger object
    """

    # Update the state of the parallel server after every assignment.
    self.parallel_server.update_state()

    # While the assignment agent's queue is not empty and there is space
    # to assign passengers in the parallel block...
    while self.parallel_server.has_space_in_a_server_queue is True and \
          len(self.queue) > 0:

      # Pop the first passenger in line and assign to the shortest queue.
      tmp = self.queue.popleft()
      self.parallel_server.min_queue.queue.append(tmp)

      # Update the state of the parallel server after every assignment.
      self.parallel_server.update_state()


class ServiceAgent(object):
  """
  Class for representing a USCBP booth agent.

  Member Data:
    online: boolean for being scheduled to be online
    id: unique sequential integer
    queue: list of Passenger objects
    is_serving: boolean for middle of a transaction
    current_passenger: pointer to current Passenger
    output_queue: pointer to ServicedPassengers object
    max_queue_size: size of max num Passengers of server queue

  Member Functions:
    serve: general service functions for completing Passenger transactions.
  """
  def __init__(self, server_id, subsection_id, output_queue):
    """
    ServiceAgent Class initialization member function.

    Args:
      server_id: integer
      output_queue: pointer to a ServicedPassengers object
    """
    self.online = False
    self.id = server_id
    self.type = subsection_id
    self.queue = deque()
    self.is_serving = False
    self.current_passenger = None
    self.output_queue = output_queue
    self.max_queue_size = 1
    self.utilization = 0.0
    self.utilization_anchor = 0
    self.utilization_series = pd.Series(np.nan,
                                        index=hourly_timestamps,
                                        name=self.id)


  def serve(self, current_time):
    """
    Process a passenger conditional on a 5-way else/if control flow.

    Args:
      current_time: global time in seconds
      write_output: boolean indicating desire to write output to csv.

    Returns:
      VOID
    """

    # If we are offline, do nothing.
    #if self.online is False: return

    # If our queue is empty and we are not serving anyone, do nothing.
    if len(self.queue) == 0 and self.is_serving is False: return

    # If we are in the middle of a transaction, do nothing.
    elif self.current_passenger and \
         self.current_passenger.departure_time > current_time: return

    # If we are not serving anyone but there are Passengers in line.
    elif self.is_serving is False and len(self.queue) > 0:

      # Pull from front of the line.
      self.current_passenger = self.queue.popleft()

      # Update our status.
      self.is_serving = True

      # Adjust the service time of the passenger.
      self.current_passenger.departure_time = current_time + \
                                             self.current_passenger.service_time

      # Return for good measure.
      return

    # We are serving a passenger and the passenger's transaction is complete.
    elif self.current_passenger and \
         self.current_passenger.departure_time == current_time:

      # Finish processing the Passenger.
      self.current_passenger.processed = True
      self.output_queue.serviced_passengers.append(self.current_passenger)
      self.output_queue.passengers_served += 1

      # Update our status.
      self.is_serving = False
      self.current_passenger = None

      # Return for good measure.
      return


  def get_utilization(self, current_time):
    """
    Method for calculating server utilization over time.

    Args:
      current_time: simulation time in sim time units

    Returns:
      VOID
    """

    # If the server is not serving, not online, and was not serving this
    # time period, move the anchor.
    if (not self.is_serving) and \
       (not self.online) and \
       (self.utilization == 0) and \
       len(self.queue) == 0:
      self.utilization_anchor = current_time

    # If the server is serving or has people waiting...
    elif self.is_serving or len(self.queue) != 0:
      if current_time == self.utilization_anchor:
        self.utilization = 1
      else:
        self.utilization = self.utilization + (
                                (1-self.utilization) /
                                ((current_time-self.utilization_anchor)*1.0))

    # If the server is online but is not doing anything...
    elif self.online and \
         (not self.is_serving) and \
         len(self.queue) == 0:
      if current_time == self.utilization_anchor:
        self.utilization = 0
      else:
        self.utilization = self.utilization + (
                                (0-self.utilization) /
                                ((current_time-self.utilization_anchor)*1.0))

    # If we are on the hour and the server has been online,
    # we flush the results and reset the utilization.
    if current_time != 0 and \
       (current_time + 1) % _get_sec("01:00:00", spd_factor) == 0 and \
       self.online:
      self.utilization_series[_get_ttime(
                  current_time + 1 - _get_sec("01:00:00", spd_factor), 
                  spd_factor)] = self.utilization


      #self.output_queue.server_statistics.append(
      #                              [self.id,
      #                               self.utilization,
      #                               _get_ttime(current_time, spd_factor)])

      self.utilization = 0
      self.utilization_anchor = current_time + 1


class Outputs(object):
  """
  Class for holding a list of Passenger objects whose transactions
  have been completed and server statistics.

  Member Data:
    passengers: python list
  """
  def __init__(self):
    """
    Outputs initialization member function.
    """
    self.serviced_passengers = deque()
    self.passengers_served = 0
    self.server_statistics = deque()


  def update_passengers(self, database, current_time):
    """
    Updates passenger service metrics in the database.

    Args:
      database: pass
      current_time: simulation time in sim time units

    Returns:
      VOID
    """

    # Check queue length or sim time.
    if len(self.serviced_passengers) >= 1000 or \
       _get_ttime(current_time, spd_factor) == "24:00:00":

      # Open connection to db.
      connection = sqlite3.connect(database)
      cursor = connection.cursor()

      # Iterate through the deque and write out.
      for passenger in self.serviced_passengers:
        #writer.writerow(list(passenger))

        cursor.execute('UPDATE passengers ' 
                         'SET enque_time = \'{enque_time}\','
                             'departure_time = \'{departure_time}\','
                             'service_time = \'{service_time}\','
                             'connecting_flight = \'{connecting_flight}\','
                             'processed = \'{processed}\''
                          'WHERE '
                             'id = \'{id}\';'
          .format(enque_time=passenger.enque_time,
                  departure_time=passenger.departure_time,
                  service_time=passenger.service_time,
                  connecting_flight=passenger.connecting_flight,
                  processed=passenger.processed,
                  id=passenger.id))

      # Clear the queue of Passenger objects.
      self.serviced_passengers.clear()

      # Close connection
      connection.commit()
      connection.close()


  def update_servers(self, output_file, current_time):
    """
    Writes out the server utilization to a CSV file in batches for
    performance.

    Args:
      output_file: file name as string for output
      current_time: simulation time in sim time units

    Returns:
      VOID
    """

    # Check the servers length list.
    if len(self.server_statistics) >= 1000 or \
       _get_ttime(current_time, spd_factor) == "24:00:00":

      # Open a context manager for the file.
      with open(output_file, 'a') as the_file:

        # Initialize a Writer object.
        writer = csv.writer(the_file, delimiter=",")

        # Iterate through the deque and write out.
        for server in self.server_statistics:
          writer.writerow(server)

      # Clear the queue of Server objects.
      self.server_statistics.clear()

