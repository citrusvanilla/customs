##
##  JFK Customs Simulation
##  customs_obj.py
##
##  Created by Justin Fung on 10/22/17.
##  Copyright 2017 Justin Fung. All rights reserved.
##
## ====================================================================

"""Objects for simulating throughput of the international arrivals
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
    h, m, s = time_str.split(':')
    return int(h) * 3600 + int(m) * 60 + int(s)

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
  """
  def __init__(self, server_architecture):
    self.serviced_passengers = ServicedPassengers()
    self.subsections = self.init_subsections(server_architecture)

  def init_subsections(self, customs_arch):
  	section_list = []

  	num_subsections = len(customs_arch['subsection'].unique())

  	for i in range(num_subsections):
  		subsection_id = customs_arch['subsection'].unique()[i]
  		subsection_block = customs_arch[customs_arch['subsection']==subsection_id])
  		serviced_passengers_list = self.serviced_passengers
  		section_list.append(Subsection(subsection_block,
  									   serviced_passengers_list))
  
  def handle_arrivals(self, plane):
  	"""
  	Method for handling a plane of arriving passengers.
  	"""
  	for section in self.subsection:

  		section.load_passengers(plane.dom_plist)



  def update_servers(self, server_schedule):
  	"""
  	Updates online/offline status of servers in parallel.
  	"""
  	# Use the global time to identify the capposite olumn of the schedule.
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
      for server in section.parallel_server.server_list:
    	matched_entry = server_schedule[server_schedule['id' = server.id]]
    	online_status = matched_entry[].iloc[:,[time_idx]].values[0][0]

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
  """
  def __init__(self, subsection_arch, serviced_passengers):
  	self.id = name
  	self.assignment_agent = AssignmentAgent(subsection_arch)
  	self.parallel_server = ParallelServer(subsection_arch, serviced_passengers)
  	
  def assign_passengers(self):
    """
	Moves a passenger to a Service Agent conditional on a set of
	requirements.

	Args:
	  passenger: a Passenger object
	
	Returns:
	  VOID: moves a Passenger object
	"""
	# Check for free service agent and assign passenger accordingly.
	if self.parallel_server.has_free_agent is True:
	  tmp = self.assignment_agent.queue.pop(0)
	  self.parallel_server.min_queue.append(tmp)

  def service_passengers(self, passenger):
	"""
	Services a passenger.
	"""
	
	# 
	for server in self.parallel_server.server_list:
	  if len(server.queue) > 0 and server.is_serving is False:
	    server.serve(server.queue.pop(0))

  def load_passengers(self, passengers_list):
  	"""
  	Fill up queues with passengers.
  	"""

  	for passenger in passengers_list:
  		self.assignment_agent.queue.append(passenger)


class ParallelServer(object):
  """
  Class representing a block of servers in parallel.
  """
  def __init__(self, subsection_arch, serviced_passengers):
    self.server_list = self.init_server_list(subsection_arch,
    										 serviced_passengers)
    self.has_free_agent = True
    self.queue_size = 0
    self.min_queue = None


  def init_server_list(self, subsection_arch, output_list):
  	server_type = subsection_arch['subsection'].unique()[0]
  	num_servers = len(subsection_arch)
  	idx = 0
  	rtn = []

  	while idx < num_servers:
  	  rtn.append(ServiceAgent(subsection_arch.loc[0,'id'],
  	  						  output_list))
  	  idx += 1

  	return rtn

  def update_min_queue(self):
  	# Identify the smallest queue in the server_list.
	min_queue = None

	for server in self.server_list:
	  if server.is_full is False and server.online is True:
	    if min_queue is None:
		  min_queue = server
		else:
		  if len(server.queue) <= len(min_queue.queue):
			min_queue = server

	return min_queue

  def update_has_free_agent(self):
  	has_free = False

  	for server in self.server_list:
	  if server.is_full is False and server.online is True:
	    has_free = True
	    break

	return has_free


class AssignmentAgent(ABCServer):
	def __init__(self, subsection_arch):
		self.online = False
		self.id = 
		self.current_passenger = 
		self.queue = []
		self.max_queue_length = None
		self.is_full = False

	
class ServiceAgent(ABCServer):
  def __init__(self, id, output_queue):
	ABCServer.__init__(self)
	self.id = id
	self.is_serving = False
	self.current_passenger = None
	self.output_queue = output_queue

  def serve(self, passenger):
	"""
	Process a passenger.

	Args:
	  passenger: a Passenger object to be served

	Returns:
	  VOID
	"""
	# We are just starting to process the passenger.
	if passenger.soujourn_time == -1:
		passenger.soujourn_time = GLOBAL_TIME + passenger.service_time
		self.is_serving = True
		self.current_passenger = passenger
	
	# The passenger is in process.
    elif passenger.soujourn_time > GLOBAL_TIME:
      # Do nothing (basically wait).
      pass

    # The passenger is done processing.
	elif passenger.soujourn_time == GLOBAL_TIME:
	  passenger.processed = True
	  self.is_serving = False
	  self.output_queue.append(passenger)
	  self.current_passenger = None


class ServicedPassengers(object):
	def __init__(self):
		self.passengers = []





