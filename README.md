# Parallel Server Scheduling for Customs Agents with Time Series-Based Greedy Optimization and Stochastic Simulation
![OOP Design](https://i.imgur.com/yeN51Rn.png)

This repository contains an end-to-end pipeline for the optimizing of Customs agent server schedules subject to maximum average wait time thresholds for agents in the system, including ETL, optimization, and modeling of the system.  Major components are written in Python 2.7.


## Software and Library Requirements
ETL:
* Python 2.7.11
* Numpy 1.11.2
* Pandas 0.19.2
* SQLite*
* Selenium 3.6.0 with html5lib, and associated web drivers
* BeautifulSoup 4.6.0
* Faker 0.8.6

Optimization:
* Python 2.7.11
* Numpy 1.11.2
* Pandas 0.19.2
* SQLite*

Analysis:
* Python 2.7.11
* Numpy 1.11.2
* Pandas 0.19.2
* SQLite*
* iPython Notebook ....


## A High-Level Overview
Scheduling Customs agents to handle incoming international arrivals is one of the biggest factors in a passenger's overall wait time, ultimately contributing to missed domestic flight connections.  The problem can be modeled as a stochastic process in which the passenger's service time by a Customs agent is sampled from a given distribution, and his/her overal wait time is a function of the number of servers working to service passengers in parallel both in the present period, and in periods past.  We present a method here that uses a time-series aware "greedy" forward search of near-optimal server scheduling to minimize the overall number of servers needed to maintain a given average wait threshold for passengers in the system.


## Program Architecture
The pipeline includes modules for data sourcing and preprocessing ("ETL"), optimization ("scheduling"), and analysis.

1. **ETL**: Scrapping, munging, faking, and loading of Customs-related data, including arrival schedules, plane types, and passenger data.
2. **Scheduling**: Server schedule optimization using repeated stochastic simulation.
3. **Analysis**: Computational comparison of optimization routine, as well as analysis of optimization results versus a simple linear heuristic method of scheduling.


## Code Organization
File | Purpose
------------ | -------------
customs.py |	Implements Multiple Wave Tracking routine.
customs_obj.py |	Definition of the Wave class and associated data members and member functions.
customs_scrape_arrivals.py |	Preproesses input video through background modeling and foreground extraction.
customs_scrape_planes.py |	Preproesses input video through background modeling and foreground extraction.
customs_passenger_generator.py |	Preproesses input video through background modeling and foreground extraction.
customs_db.sqlite  |  db
output/  |  
schedules/  |  


## The Customs Pipeline, in short
Though the model can be reasonably ported to any international customs system, we have chosen to demonstrate our method on the international arrivals for Terminal 4 (International arrivals) of the the JFK Airport in New York City, New York.

* **ETL**: An arrival schedule for a given data in the present or in the future is retrieved from the official JFK website via Python 'requests' HTML library.  The HTML is parsed using the Document Object Model (DOM) description of HTML using BeautifulSoup, and a full arrival schedule of both domestic and international arrivals is extracted and loaded into an embedded SQLite database.  The data about each arrival's flight number is used as input into a module that scrapes HTML from the SeatGuru website corresponding to the airline carrier's seat arrangement for the given body type.  BeautifulSoup is again utilized to extract relevant containers from the DOM, and this data about the plane's seating is loaded to the same embedded SQLite database.  Finally, a separate passenger generator script is provided that makes assumptions about the proportion of domestic-versus-international passengers and loads "fake" data about an individual passenger into the SQLite database for every scheduled arrival.
* **Optimization/Scheduling**: The Customs systems is modeled using common Object-oriented principles (see the lead illustration above).  The Python program pulls relevant data from the embedded SQLite database, often in vector-optimized Pandas DataFrames, and loads data into the customs pipeline.  Passenger throughput is simulated with random service times pulled from triangular distributions.  Passengers are updated and simulation metrics are written back to the database.  Common SQL analysis queries provide separate output files that return optimized schedules, heuristic schedules, and program logs, all in friendly CSV format.
* **Analysis**: Comparison between heuristic scheduling and optimized scheduling is performed in iPython notebook.  Sensitivity of number of scheduled servers to average time thresholds is explored.  Discussion and narrative is provided.


## Data and Assumptions
The time-based greedy optimization method can reasonably be ported to any number of problem formulations in which a parallel server block represents one bottleneck in an agent's soujourn time.  For our purposes, we have chosen to demonstrate the effectiveness of this approach on the scheduling of United States Customs Border Patrol (USCBP) agents at the international arrivals terminal of the JFK airport in New York City, New York.

The schedule of all arrivals is pulled from the official JFK website.  This schedule contains flight numbers, arrival times, and carriers.  We determine the number of total arriving passengers per plane by using the flight numbers and arrival dates as input to SeatGuru's seat map database, which returns the number of passenger seats for the given airline and airplane type.  We do not know the real capacity of each plane arriving, so we uniformly choose a percentage between 80% and 100% for each arrival.

For our simulation, the passenger manifest per plane is unknown, so we choose a random split of domestic and international passengers for each plane, in which the mode is shifted 60/40 in favor of international passengers.  This number is derived from a re-engineering of given average arrival times for both domestic and international passengers that comes from the offical USCBP website.

Stochasticity for domestic/international splits per plane is determined during ETL.  Therefore, in the main simulation, for every successive time unit, we lookup a potential arrival from a schedule handler.  If a plane is due, the simulation will pull all passengers from the database and pass them to their respective subsections ('domestic' and 'international'), whereby they have entered the simulation and are subject to service by the parallel server blocks.

It is assumed from the standpoint of USCBP that the stochasticity of a passenger's individual service time is unknown until service is actually realized.  We further assume that the passenger service time distribution is triangular and long-tailed, and that the domestic distribution is shorter in duration relative to the international service distribution.  This assumption is supported again by the USCBP website.

Servers are due to be scheduled per hour block, so that a simulation of 24 hours of the system will see servers schedules according to 24 different time blocks.  Every parallel server block has a maximum number of servers that can be scheduled at any time and for this problem we choose to raise the maximum number of servers so that it does not significantly affect the optimization behavior- in practice the number of service booths that can physically be open at any one time is a hard requirement.

For evaluation sake, we assume that a naive comparison schedule would see the scheduling of the same number of servers as in the optimized approach, only distributed according to a simple linear heuristic, such as "1 online server per 40 arriving passengers per hour".


## Launching the Model
There is one non-file argument that should be passed to the command line for execution, and that is the average passenger wait threshold that the optimization routine optimizes for.  This will attempt to ensure that servers are scheduled and returned so that the average wait time of a passenger that arrives in any time block has a wait that does not exceed this argument.

You can launch the program from the command line after navigating to the parent directory and entering the following command.

> joe_bloggs:~/customs$ python customs.py 20

You should see output like this:

        ===================================================================
        Current server schedule:
        ave_server_utilization ave_wait count hour max_wait num_servers      type
        0                   0.23        4   165    0        9          15  domestic
        0                   0.07        2    52    1        5          15  domestic
        ...
    

The program will report simple statistics at the conclusion of the optimization, like the following:

        ===================================================================
        Optimized model complete.  Written to output/optimized_models.csv.
        107 simulations performed in 265.797832012 seconds.
        ===================================================================

To run repeated simulations, each with a different realization of every passenger's service time, you can simply run the above command in a loop, as in the following:

> for i in {1..50} do python customs.py 20 done

Simulation results ("outputs") will continuously be appended to the bottom of the following files:


## Output Files
* output/log.csv: A log of the tracking routine is written to output/log.csv and contains the time in seconds since epoch, number of total simulations needed to arrival at the near-optimal schedule, and the average time in seconds required to run an individual simulation.
* output/optimized_models.csv: A schedule, broken down per hour, of the number of passengers scheduled to arrive, the average wait for a passenger in the system that arrives in that hour, the maximum wait of all passengers that are scheduled to arrive in that hour, the average server utilization of an online server in that hour, and the optimized number of scheduled servers for that hour.
* output/heuristic_models.csv: A schedule, broken down per hour, of the number of passengers scheduled to arrive, the average wait for a passenger in the system that arrives in that hour, the maximum wait of all passengers that are scheduled to arrive in that hour, the average server utilization of an online server in that hour, and the heuristic number of scheduled servers for that hour.

Schedules for both the optimized and heuristic cases will be formatted like the following:

![Schedule Example](https://i.imgur.com/dhV1hQb.jpg)


## Customs Model Discussion
The customs system is modeled using basic Object Oriented (OO) principle (see diagram above).  An arrivals handler class ("PlaneDispatcher") is in charge of reading schedules and querying flight manifests from the embedded SQLite database.  With this information, the handler class creates and dispatches Plane objects, as well as creates the Passenger objects that each Plane contains.

The Customs class contains two Subsection objects- one each for the domestic subsystem and the international subsystem.  Each subsection has an AssignmentAgent class in charge of holding main queues of passengers, and assigning Passenger objects to individual Server objects.  All Server objects are contained in ParallelServer objects.

The Server object services the passenger and "sends" passengers to a ServicedPassengers list/object upon completion of service.


## Greedy Optimization Discussion
The parameter space for this optimization problem is exponential in the number of time blocks and number of potential servers per time block.  For example, if we want to choose between 1 and 20 servers for 24 successive time periods, we have a potential complexity of O(20^24) ~ 1.67 * 10^31 different possibilities, which is a number much, much larger than the number of grains of sand on planet earth.  The time-based greedy optimization method does a forward search through the simulation, optimizing the number of servers only for the current time period, while ensuring that all previous average time restraints are not violated.  This method reduces the complexity to a linear problem in O(20 * 24) ~ 480.

The pseudo-code for this method looks like the following:

For each successive hour in the simulation:
1. Adjust the number of servers in the current time period and every time period till the end of the simulation so that the average wait time of a passenger in the current time period is maximized, subject to the restraint that the wait time falls under the time threshold.
2. If the restraints in any time period prior to the current one are violated, backtrack the number of servers in the current time period and future time periods so that the past restraints still hold.
3. If the restraints do not hold but the number of servers is at a maximum, break.
4.  Fix number of servers in current period, and move to next time period.

The "search" for the number of servers uses a method of gradient descent with momentum.  We start by skipping every n-th server in our serach.  If we move closer to our targeted average wait time while adjusting the number of servers in the same direction in successive steps, we continue "skipping".  If we exceed or fall under our threshold, we backtrack the number of servers without skipping until we find the number of servers that maximizes the average wait time subject to falling under a given threshold.

To illustrate, we'll start with a brand new theoretical simulation in which we want to maintain an average wait time for all passengers of less than 20 minutes.  We start with time period 0, which we will call the hour 12:00 midnight to 1:00am.  We initialize the number of servers for period 0 and every period after to be flat at the maximum number of servers possible, which we will say is 15 servers.  At this level, we observe an average wait time of only 5 minutes for passengers arriving between 12 and 1am.  In this case, we can probably afford to reduce the number of online servers while still maintaining a wait time of less than 20 minutes, so we reduce the number of servers for time period 0 and every time period after until the average wait in time period 0 is maximized, but still under the threshold.  So for example, in our case we find that 5 online servers in the current time period and every time period after gives us an average wait time of 19 minutes for passengers arriving from 12a to 1am.  We fix the number of servers then for this time period at 5, and move onto the next time period.  In the next time period (period 1, which covers 1am to 2am), we see that 5 servers is causing passengers that have arrived in time period 1 to experience an average wait of 27 minutes, which exceeds our threshold.  We therefore adjust the number of servers in time period 1 and every time period thereafter upward until our greedy optimization goal has been reached.  In our example, we find that this is achieved in time period 1 when there are 8 servers online now and for every time period after.  We fix this number of online servers for period 1 at 8, so that we have a final schedule of 5 servers online in time period 0, 8 servers online in time period 1, and TBD servers going forward.  We move to time period 2 and repeat this process until the simulation ends.


## Program Performance Discussion
The Optimzation program contained in customs.py is a single-threaded pipeline that make numerous queries and updates of a light-weight embedded SQLite database.  I/O output is restricted to writing CSVs upon program completion.

On AWS-optimized 2.6Ghz Intel Xeon E5-2666 v3 processors, a single pass of the 24 hour simulation takes about 2-3 seconds.  A full schedule optimization for all 24 hours may comprise anywhere between 100 and 200 simulations, depending on the maximum number of servers that are being evaluated, resulting in an optimization runtime of 4-10 minutes.  To embrace stochastic programming principles, both individual simulations for evaluation purposes and optimizations for scheduling purposes should be run numerous time and their results 
ensembled.  Run time for ensembling therefore scales linearly with the number of repretitions required.  It is suggested to run the program on as many CPUs cores available for parallelization.
