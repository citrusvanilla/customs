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

# Macros
PASSENGER_COUNT = pass
GLOBAL_TIME = pass
PLANE_COUNT = 0

# Service Distributions
service_dist_dom = ("00:30","00:45","02:00")
service_dist_intl = ("00:45","01:00","02:15")

# Helper functions
def _get_sec(time_str):
    m, s = time_str.split(':')
    return int(m) * 60 + int(s)

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


# Classes

class PlaneDispatcher(object):
  def __init__(self, schedule):
    self.schedule = schedule

  def dispatch_planes(self):
    # Init new plane
    new_plane = None

    # Generate arrivals
    if GLOBAL_TIME == schedule.loc[PLANE_COUNT, 'arrival_time']:
        # New Plane arrives
        new_plane = Plane(arr_sched.loc[PLANE_COUNT, 'plane_id'],
                          arr_sched.loc[PLANE_COUNT, 'passengers_dom']
                          arr_sched.loc[PLANE_COUNT, 'passengers_intl']
                          'dom',
                          'intl')
        PLANE_COUNT += 1

    return new_plane


class Plane(object):
	def __init__(self, id, num_passengers1, num_passengers2,
		         passenger_type1, passenger_type2):
		self.id = id
		self.dom_plist = self.init_plist(num_passengers1, passenger_type1)
		self.intl_plist = self.init_plist(num_passengers2, passenger_type2)

	def init_plist(self, num_passengers, passenger_type):
		rtn = []
		counter = 1
		global PASSENGER_COUNT

		while counter <= num_passengers:
			rtn.append(Passenger(str(PASSENGER_COUNT), passenger_type))
			counter += 1
			PASSENGER_COUNT += 1

		return rtn


class Passenger(object):
  def __init__(self, id, nationality):
	self.id = id
	self.nationality = nationality
	self.enque_time = GLOBAL_TIME
	self.soujourn_time = -1
	self.service_time = init_service_time
	self.connect_flight = False
	self.processed = False

  def init_service_time(self):
  	if self.nationality == "dom":
  	  return sample_from_triangular(service_dist_dom)
  	elif self.nationaliy == "intl":
  	  return sample_from_triangular(service_dist_intl)


#######################################################################
#######################################################################
#######################################################################


class Customs(object):
  """
  Wrapper class representing the Customs system.
  """
  def __init__(self, customs_dataframe):
    self.serviced_passengers = ServicedPassengers()
    self.dom_section = Subsection(customs_dataframe.loc[0,"subsection_name"],
							      customs_dataframe.loc[0,"maxnum_servers"],
	  						      self.serviced_passengers))
    self.intl_section = Subsection(customs_dataframe.loc[1,"subsection_name"],
							       customs_dataframe.loc[1,"maxnum_servers"],
	  						       self.serviced_passengers))
  
  def handle_arrivals(self, plane):
  	"""
  	Method for handling a plane of arriving passengers.
  	"""
  	# Add domestic passengers to the Assignment Agent's queue.
  	self.dom_section.load_passengers(plane.dom_plist)

  	# Add international passengers to the Assignment Agent's queue.
  	self.intl_section.load_passengers(plane.intl_plist)


class Subsection(object):
  """
  Class representing subqueues of a Customs system.  Traditionally, a
  Customs system would consist of two or three subsections of which one
  is a domestic processing queue for nationals and the other is an
  international processing queue for aliens.
  """
  def __init__(self, name, maxnum_servers, serviced_passengers):
  	self.id = name
  	self.assignment_agent = AssignmentAgent()
  	self.parallel_server = ParallelServer(maxnum_servers, serviced_passengers)
  	

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
  def __init__(self, num_servers, serviced_passengers):
    self.server_list = self.init_server_list(num_servers, serviced_passengers)
    self.has_free_agent = True
    self.queue_size = 0
    self.min_queue = None


  def init_server_list(self, k, output_list):
  	counter = 1
  	rtn = []

  	while counter <= k:
  	  rtn.append(ServiceAgent(output_list))
  	  counter += 1

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


class ABCServer(object):
	def __init__(self):
		self.online = False
		#self.current_passenger = None
		self.queue = []
		self.max_queue_length = None
		self.is_full = False


class AssignmentAgent(ABCServer):
	def __init__(self):
		ABCServer.__init__(self)

	
class ServiceAgent(ABCServer):
  def __init__(self, output_queue):
	ABCServer.__init__(self)
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





