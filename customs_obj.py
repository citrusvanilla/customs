##
##  JFK Customs Simulation
##  customs_obj.py
##
##  Created by Justin Fung on 10/22/17.
##  Copyright 2017 Justin Fung. All rights reserved.
##
## ====================================================================
"""
Objects for simulating throughput of the international arrivals
customs at JFK airport.  The customs system is modeled through OO
construction.

Usage:
  Please see the README for how to compile the program and run the model.
"""
import re


# Macros
PASSENGER_ID = 0

# Service Distributions
service_dist_dom = ("00:00:30","00:00:45","00:02:00")
service_dist_intl = ("00:00:45","00:01:00","00:02:15")

# Helper functions
def _get_sec(time_str):
    """
    Convert a string in "HH:MM:SS" format to seconds as an integer.

    Args:
      time_str: a string in "HH:MM:SS" format

    Returns:
      seconds: an integer
    """
    h, m, s = time_str.split(':')
    seconds = int(h) * 3600 + int(m) * 60 + int(s)
    
    return seconds

def sample_from_triangular(service_dist):
  """
  Returns number of seconds.

  Args:
    service_dist: tuple of lower, middle, upper parameters of a
                  triangular distribution as strings in MM:SS format.

  Returns:
    sample: service time in seconds as integer

  """
  lower = _get_sec(lower)
  mode = _get_sec(mode)
  upper = _get_sec(upper)

  sample = int(np.random.triangular(lower,mode, upper))

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
  def __init__(self, arrival_schedule):
    """
    PlaneDispatcher must be instantiated with an arrivals schedule
    whose format is specified in the README.  Counts are set to zero.
    """
    self.schedule = arrival_schedule
    self.plane_count = 0
    self.passenger_count = 0

  def dispatch_plane(self, global_time):
    """
    PlaneDispatcher class method for initializing and returning a new
    plane on schedule.

    Args:
      global_time: simulation time in seconds.

    Returns:
      new_plane: a Plane object
    """
    # If global time coincides with an arrival, generate a new plane.
    if global_time == _get_sec(schedule.loc[self.plane_count, 'arrival_time']):
      # Init new Plane object with details from the schedule.
      new_plane = Plane(schedule.iloc[self.plane_count], global_time)

      # Increment counts for planes and passengers dispatched.
      self.plane_count += 1
      self.passenger_count += len(new_plane.plist)

      return new_plane


class Plane(object):
  """
  Class representing an arriving Plane.

  Member Data:
    id = id of the plane as a unique, sequential integer
    plist = a list holding Passenger objects representing passengers
    global_time = simulation arrival time

  Member Fuctions:
    init_plist: initializes a list of passengers upon instantiation
                of the Plane object
  """
  def __init__(self, plane_series, global_time):
    """
    Plane class must be instantiated with a Pandas series representing
    plane data.
    """
    self.id = plane_series['plane_id']
    self.plist = self.init_plist(plane_series, global_time)
    self.arrival_time = global_time

  def init_plist(self, plane_series, global_time):
    """
    Plane class member function for initializing a list of passenger
    objects.

    Args:
      plane_series: a pandas series
      global_time: simluation time in seconds

    Returns:
      rtn: a list of Passenger objects.
    """
    rtn = []

    # Suss out counts of domestic and international passengers per plane.
    for header in plane_series.index:
        if re.search('passengers', header):
            if re.search('dom', header):
                num_passengers_dom = plane_series[header]
            elif re.search('intl', header):
                num_passengers_intl = plane_series[header]

    # Add domestic passengers to the passenger list.
    for i in range(num_passengers_dom):
      run.append(Passenger('dom', global_time))

    # Add international passengers to the passenger list.
    for i in range(num_passengers_intl):
      rtn.append(Passenger('intl', global_time))

    return rtn


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
  def __init__(self, nationality, global_time):
    """
    Passenger class must be initialized with nationality and global time.
    """
    self.id = self.init_id()
    self.nationality = nationality
    self.enque_time = global_time
    self.soujourn_time = -1
    self.service_time = self.init_service_time()
    self.connect_flight = False
    self.processed = False

  def init_id(self):
    """
    Initialize a passenger ID with a global value.
    """
    global PASSENGER_ID
    self.id = PASSENGER_ID
    PASSENGER_ID += 1

  def init_service_time(self):
    """
    Generate a random service time from a passed distribution.
    """
    if self.nationality == "dom":
      return sample_from_triangular(service_dist_dom)
    elif self.nationaliy == "intl":
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
        subsection_arch = customs_arch[customs_arch['subsection']==subsection_id]
        # Get the processed passenger queue from the Class Data Members list.
        serviced_passengers_list = self.serviced_passengers
        # Init a subsection and append to the list.
        section_list.append(Subsection(subsection_arch,
                                       serviced_passengers_list))

    return section_list
  
  def handle_arrivals(self, plane):
    """
    Method for handling a plane of arriving passengers.

    Args:
      plane: an initialized Plane object

    Returns:
      VOID
    """
    # Immediately return if plane is null.
    if plane is None:
        return

    # Loop through all the passengers in the plane.
    while len(plane.plist) > 0:
        # Loop through all the sections.
        for section in subsections:
            # Add Passenger to the correct AssignmentAgent queue.
            if plane.plist[len(plane.plist)-1].nationality == section.id:
                section.assignment_agent.queue.append(plane.plist.pop())
                break


  def update_servers(self, server_schedule):
    """
    Updates online/offline status of servers in parallel.

    Args:
      server_schedule: a Pandas dataframe

    Returns:
      VOID
    """
    # Use the global time to identify the apposite column of the schedule.
    global GLOBAL_TIME
    time_idx = None

    # Loop through the dataframe columns to find the correct column.
    for idx, col in enumerate(server_schedule.columns):
      if re.search('[0-9]-[0-9]', col):
        if _get_sec(col.split('-')[0] + ":00:00") <= GLOBAL_TIME <=
           _get_sec(col.split('-')[1] + ":00:00"):
           time_idx = idx
           break

    # Loop through all subsections.parallel_server.server_list:
    for section in self.subsections:
      # Loop through every server in the server list.
      for server in section.parallel_server.server_list:
        # Find the row corresponding to the server in the server schedule.
        matched_entry = server_schedule[server_schedule['id'] == server.id]
        # Extract the status of the server using the global time.
        online_status = matched_entry[].iloc[:,[time_idx]].values[0][0]
        # Update the server status.
        if online_status == 1:
          server.is_serving = True 
        else:
          server.is_serving = False


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
  def __init__(self, subsection_arch, serviced_passengers):
    """
    Subsection Class initialization function.

    Args:
      subsection_arch: a Pandas dataframe
      serviced_passengers: a python list
    """
    self.id = name
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
    self.queues_size = 0
    self.min_queue = None

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
    # Metadata of the subsection_arch dataframe.
    server_type = subsection_arch['subsection'].unique()[0]
    num_servers = len(subsection_arch)
    
    idx = 0
    rtn = []

    # Loop through all servers in the arch.
    while idx < num_servers:
      rtn.append(ServiceAgent(subsection_arch.loc[idx,'id'],
                              output_list))
      idx += 1

    return rtn

  def service_passengers(self):
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
      # If there are Passengers in the queue and the booth is empty,
      # ServiceAgent should begin service on next Passenger in the queue.
      if len(server.queue) > 0:
        server.serve()

  def update_min_queue(self):
    """
    ParallelServer Class member function that updates the identity of
    the shortest queue in the block that is still online.

    Args:
      None

    Returns:
      min_queue: a pointer to a ServiceAgent object
    """
    # Identify the smallest queue in the server_list.
    min_queue = None

    for server in self.server_list:
      if len(server.queue) < server.max_queue_size and server.online is True:
        if min_queue is None:
          min_queue = server
        else:
          if len(server.queue) <= len(min_queue.queue):
            min_queue = server

    return min_queue

  def update_has_space_in_a_server_queue(self):
    """
    ParallelServer Class function for updating whether or not a Passenger
    can be moved from the AssignmentAgent queue to a ServiceAgent queue.

    Args:
      None

    Returns:
      has_space: boolean
    """
    has_space = False

    for server in self.server_list:
      # Check for an online server queue smaller than max size.
      if len(server.queue) < server.max_queue_size and server.online is True:
        has_space = True
        break

    return has_space


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
  def __init__(self, parallel_server_obj):
    """
    AssignmentAgent Class initialization Member Function.

    Args:
      parallel_server_obj: an initialized ParallelServer object
    """
    self.queue = []
    self.current_passenger = self.queue[0] if len(self.queue) > 0 else None

  def assign_passengers(self, parallel_server_obj):
    """
    AssignmentAgent Class member function that moves a passenger to a 
    Service Agent conditional on a set of requirements.

    Args:
      parallel_server_obj: a Passenger object
    
    Returns:
      VOID: moves a Passenger object
    """
    # Check for free service agent and assign passenger accordingly.
    if parallel_server_obj.has_space_in_a_server_queue is True:
      tmp = self.assignment_agent.queue.pop(0)
      self.parallel_server.min_queue.append(tmp)

    
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
    self.queue = []
    self.is_serving = False
    self.current_passenger = None
    self.output_queue = output_queue
    self.max_queue_size = None

  def serve(self):
    """
    Process a passenger.

    Args:
      passenger: a Passenger object to be served

    Returns:
      VOID
    """
    # We are not serving anyone but there are Passengers in line.
    if self.is_serving is False:
        self.current_passenger = self.queue.pop(0)
        self.is_serving = True
        passenger.soujourn_time = GLOBAL_TIME + passenger.service_time

    # We are serving someone.
    if self.is_serving is True:
      # We are in the middle of a transaction:
      if passenger.soujourn_time > GLOBAL_TIME:
        # Do nothing (basically wait).
        pass

      # The passenger's transaction is complete.
      elif passenger.soujourn_time == GLOBAL_TIME:
        # Finish processing the Passenger.
        passenger.processed = True
        self.output_queue.append(passenger)
        # Update our status.
        self.is_serving = False
        self.current_passenger = None


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
      self.passengers = []

