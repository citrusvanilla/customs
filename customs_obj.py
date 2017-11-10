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
  Please see the README for how to compile the program and run the model.
"""
from __future__ import print_function
from collections import deque

import re
import sqlite3
import numpy as np


## ====================================================================

# Service Distributions
service_dist_dom = ("00:00:30", "00:00:45", "00:02:00")
service_dist_intl = ("00:00:45", "00:01:00", "00:02:15")

# Speed up factor
spd_factor = 10

# Helper functions
def _get_sec(time_str, speed_factor):
  """
  Convert a string in "HH:MM:SS" format to seconds as an integer.

  Args:
    time_str: a string in "HH:MM:SS" format

  Returns:
    seconds: an integer
  """
  h, m, s = time_str.split(':')
  seconds = int(h) * 3600 + int(m) * 60 + int(s)

  # Adjust for speed.
  seconds = seconds / speed_factor

  return seconds


def _get_ttime(seconds, speed_factor):
  """
  Convert seconds as an integer to "HH:MM:SS" format.

  Args:
    seconds: an integer

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
  lower = _get_sec(service_dist[0], spd_factor)
  mode = _get_sec(service_dist[1], spd_factor)
  upper = _get_sec(service_dist[2], spd_factor)

  sample = int(np.random.triangular(lower, mode, upper))

  #sample = 

  return sample


## ====================================================================


class PlaneDispatcher(object):
  """
  Hanlder Class for managing arrivals schedule and building planes and
  their passengers in accordance with the schedule.  Should be
  instantiated only one per simulation.

  Member Data:
    schedule: holds a pandas dataframe of arrivals and passenger details
    plane_count: simple integer count of planes initialized
    passenger_count: simple integer count of passengers initialized

  Member Functions:
    dispatch_plane: returns initialized planes if simulation time
                    matches an arrival.
  """
  def __init__(self, sqlite_database):
    """
    PlaneDispatcher must be instantiated with an arrivals schedule
    whose format is specified in the README.  Counts are set to zero.
    """
    self.connection = sqlite3.connect(sqlite_database)
    self.cursor = self.connection.cursor()
    #self.schedule = arrival_schedule
    self.plane_count = 0
    self.passenger_count = 0


  def dispatch_planes(self, global_time):
    """
    PlaneDispatcher class method for initializing and returning a new
    plane on schedule.

    Args:
      global_time: simulation time in seconds.

    Returns:
      new_plane: a Plane object
    """
    # Init a list to hold the plane objects.
    planes = []

    # Query database for a potential international arrivals.
    arrivals = self.cursor.execute('SELECT arrivals.id, '
                                       'arrivals.origin, '
                                       'arrivals.airport_code, '
                                       'arrivals.arrival_time, '
                                       'arrivals.airline, '
                                       'arrivals.flight_num, '
                                       'arrivals.terminal '
                                   'FROM arrivals LEFT JOIN airports '
                                   'ON arrivals.airport_code = airports.code '
                                   'WHERE arrivals.code_share = \'\' '
                                   'AND arrivals.arrival_time = \'{time}\' '
                                   'AND airports.country != \"United States\";'\
                    .format(time=_get_ttime(global_time, spd_factor))).fetchall()

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
                                     'nationality '
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
    Overwrite default destroyer method.
    '''
    self.connection.close()


class Plane(object):
  """
  Class representing an arriving Plane.

  Member Data:
    self.id = plane_id
    self.origin = origin
    sefl.airport_code = airport_code
    self.arrival_time = arrival_time
    self.airline = airline
    self.flight_num = flight_num
    self.terminal = terminal
    self.plist = self.init_plist(plist)

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
      plane_series: a pandas series
      global_time: simluation time in seconds

    Returns:
      rtn: a list of Passenger objects.
    """
    # Init empty list to hold Passenger objects.
    plist = []

    # Iterate through the flight manifest.
    for passenger in passenger_list:

      # Grab attributes.
      pid, flight_num, first_name, \
      last_name, birthdate, nationality = passenger

      # Init and append Passenger object with attributes.
      plist.append(Passenger(pid,
                             flight_num,
                             self.arrival_time,
                             first_name,
                             last_name,
                             birthdate,
                             nationality))

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
    id = unique sequential integer id
    nationality = foreign/domestic designation
    enque_time = time of arrival to customs
    soujourn_time = total time in the system
    service_time = time of service by service agent
    conntect_flight = boolen for having a connecting conntect_flight
    processed = whether or not the passenger has been serviced_passengers

  Member Functions:
    init_id: initialized id member with unique identifier.
    init_service_time: update function to indicate that service has begun.
  """
  def __init__(self, pid, flight_num, arrival_time, first_name, last_name,
               birthdate, nationality):
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
    self.soujourn_time = -1
    self.service_time = self.init_service_time()
    self.connect_flight = False
    self.processed = False


  def init_service_time(self):
    """
    Generate a random service time from a passed distribution.
    """
    if self.nationality == "domestic":
      return sample_from_triangular(service_dist_dom)
    elif self.nationality == "foreign":
      return sample_from_triangular(service_dist_intl)


## ====================================================================


class Customs(object):
  """
  Wrapper class representing the Customs system.

  Member Data:
    serviced_passengers: a class that holds all processed passengers
    subsections: a list of subsections containing parallel servers

  Member Functions:
    init_subsections: initializes list of Subsections objects
    handle_arrivals: accepts Plane objects and fills queues with Passengers
    update_servers: updates individual servers online/offline statuses
  """
  def __init__(self, server_architecture):
    """
    Customs Class initialization member function.

    Args:
      server_architecture: a pandas dataframe representing the
                           architecture of the servers
    """
    self.serviced_passengers = ServicedPassengers()
    self.subsections = self.init_subsections(server_architecture)


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

    # Initialize each Subsection Class with a loop.
    for i in range(num_subsections):
        
      # Get the label of the subsection.
      subsection_id = customs_arch['subsection'].unique()[i]
      
      # Subset the master architecture into an architecture just for
      # for the subsection.
      subsection_arch = customs_arch[customs_arch['subsection'] == subsection_id]
      
      # Get the processed passenger queue from the Class Data Members list.
      serviced_passengers_list = self.serviced_passengers

      # Init a subsection and append to the list.
      section_list.append(Subsection(subsection_id,
                                     subsection_arch,
                                     serviced_passengers_list))

    return section_list


  def handle_arrivals(self, planes):
    """
    Method for handling a plane of arriving passengers.

    Args:
      plane: an initialized Plane object

    Returns:
      VOID
    """
    # Immediately return if there are no planes to handle.
    if not planes: return

    # Get the subsection indices for dom/intl queues.
    id_domestic = 0 if self.subsections[0].id == 'domestic' else 1
    id_foreign = 0 if self.subsections[0].id == 'foreign' else 1

    # Loop through the list of Planes.
    for plane in planes:
      print ("+ Added ", len(plane.plist), " passengers from flight ",
             plane.flight_num, sep = "")

      # While the plane still has passengers on it...
      while len(plane.plist) > 0:

        # If national, move to national queue.
        if plane.plist[-1].nationality == "domestic":
          self.subsections[id_domestic].assignment_agent.queue.append(
                                                            plane.plist.pop())

        # If intl, move to intl queue.
        elif plane.plist[-1].nationality == "foreign":
          self.subsections[id_foreign].assignment_agent.queue.append(
                                                            plane.plist.pop())


  def update_servers(self, server_schedule, global_time):
    """
    Updates online/offline status of servers in parallel.

    Args:
      server_schedule: a Pandas dataframe

    Returns:
      VOID
    """
    # Use the global time to identify the apposite column of the schedule.
    time_idx = None

    # Loop through the dataframe columns to find the correct column.
    for idx, col in enumerate(server_schedule.columns):
      if re.search('[0-9]-[0-9]', col):
        if _get_sec(col.split('-')[0] + ":00:00", spd_factor) <= global_time <= \
           _get_sec(col.split('-')[1] + ":00:00", spd_factor):
           time_idx = idx
           break

    # Loop through all subsections.parallel_server.server_list:
    for section in self.subsections:
      # Loop through every server in the server list.
      for server in section.parallel_server.server_list:
        # Find the row corresponding to the server in the server schedule.
        matched_entry = server_schedule[server_schedule['id'] == server.id]
        # Extract the status of the server using the global time.
        online_status = matched_entry.iloc[:, [time_idx]].values[0][0]
        # Update the server status.
        if online_status == 1:
          server.online = True
        else:
          server.online = False


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
  def __init__(self, subsection_id, subsection_arch, serviced_passengers):
    """
    Subsection Class initialization function.

    Args:
      subsection_arch: a Pandas dataframe
      serviced_passengers: a python list
    """
    self.id = subsection_id
    self.parallel_server = ParallelServer(subsection_arch, serviced_passengers)
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
  def __init__(self, subsection_arch, serviced_passengers):
    """
    ParallelServer Class initialization member function.

    Args:
      subsection_arch: a Pandas dataframe
      serviced_passengers: a python list
    """
    self.server_list = self.init_server_list(subsection_arch,
                                             serviced_passengers)
    self.has_space_in_a_server_queue = True
    self.queue_size = 0
    self.min_queue = self.server_list[0]

  def init_server_list(self, subsection_arch, output_list):
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
    for _, row in subsection_arch.iterrows():

      # Get the ID of the server and Init a server.
      rtn.append(ServiceAgent(row['id'], output_list))

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
    ParallelServer Class member function that updates the identity of
    the shortest queue in the block that is still online.

    Args:
      None

    Returns:
      min_queue: a pointer to a ServiceAgent object
    """
    # Start off assuming no space in the queues and no pointer to a
    # shortest queue.
    self.min_queue = None
    self.has_space_in_a_server_queue = False
    self.queue_size = 0

    # Loop through all the servers.
    for server in self.server_list:

      # If any server has space and is online...
      if len(server.queue) < server.max_queue_size and server.online is True:

        # 'Has Space' is True and remains true.
        if self.has_space_in_a_server_queue is False:
          self.has_space_in_a_server_queue = True

        # First non-full server we come to.
        if self.min_queue is None:
          self.min_queue = server

        # If we already had a non-full queue in hand, compare the present one.
        elif len(server.queue) < len(self.min_queue.queue):
          self.min_queue = server

      # Increment the count of the parallel server block.
      self.queue_size += len(server.queue)


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

    Args:
      parallel_server_obj: an initialized ParallelServer object
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
  def __init__(self, server_id, output_queue):
    """
    ServiceAgent Class initialization member function.

    Args:
      server_id: integer
      output_queue: pointer to a ServicedPassengers object
    """
    self.online = False
    self.id = server_id
    self.queue = deque()
    self.is_serving = False
    self.current_passenger = None
    self.output_queue = output_queue
    self.max_queue_size = 10


  def serve(self, current_time):
    """
    Process a passenger conditional on a 5-way else/if control flow.

    Args:
      current_time: global time in seconds

    Returns:
      VOID
    """
    # If we are offline, do nothing.
    if self.online is False: return

    # If our queue is empty and we are not serving anyone, do nothing.
    elif len(self.queue) == 0 and self.is_serving is False: return

    # If we are in the middle of a transaction, do nothing.
    elif self.current_passenger and \
         self.current_passenger.soujourn_time > current_time: return

    # If we are not serving anyone but there are Passengers in line.
    elif self.is_serving is False and len(self.queue) > 0:

      # Pull from front of the line.
      self.current_passenger = self.queue.popleft()

      # Update our status.
      self.is_serving = True

      # Adjust the service time of the passenger.
      self.current_passenger.soujourn_time = current_time + \
                                             self.current_passenger.service_time

      # Return for good measure.
      return

    # We are serving a passenger and the passenger's transaction is complete.
    elif self.current_passenger and \
         self.current_passenger.soujourn_time == current_time:

      # Finish processing the Passenger.
      self.current_passenger.processed = True
      self.output_queue.queue.append(self.current_passenger)

      # Update our status.
      self.is_serving = False
      self.current_passenger = None

      # Return for good measure.
      return


class ServicedPassengers(object):
    """
    Class for holding a list of Passenger objects whose transactions
    have been completed.

    Member Data:
      passengers: python list
    """
    def __init__(self):
      """
      ServicedPassengers initialization member function.
      """
      self.queue = deque()

