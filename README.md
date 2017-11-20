# Parallel Server Scheduling for Customs Agents with Time Series-Based Greedy Optimization and Stochastic Simulation

![OOP Design](https://i.imgur.com/yeN51Rn.png)
This repository contains an end-to-end pipeline for the optimizing of Customs agent server schedules subject to maximum average wait time thresholds for agents in the system, including ETL, optimization, and modeling of the system.  Major components are writting in Python 2.7.

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

## The Customs pipeline, in short

Though the model can be reasonably ported to any international customs system, we have chosen to demonstrate our method on the international arrivals for Terminal 4 (International arrivals) of the the JFK Airport in New York City, New York.

* **ETL**: An arrival schedule for a given data in the present or in the future is retrieved from the official JFK website via Python 'requests' HTML library.  The HTML is parsed using the Document Object Model (DOM) description of HTML using BeautifulSoup, and a full arrival schedule of both domestic and international arrivals is extracted and loaded into an embedded SQLite database.  The data about each arrival's flight number is used as input into a module that scrapes HTML from the SeatGuru website corresponding to the airline carrier's seat arrangement for the given body type.  BeautifulSoup is again utilized to extract relevant containers from the DOM, and this data about the plane's seating is loaded to the same embedded SQLite database.  Finally, a separate passenger generator script is provided that makes assumptions about the proportion of domestic-versus-international passengers and loads "fake" data about an individual passenger into the SQLite database for every scheduled arrival.
* **Scheduling**: The Customs systems is modeled using common Object-oriented principles (see the lead illustration above).  The Python program pulls relevant data from the embedded SQLite database, often in vector-optimized Pandas DataFrames, and loads data into the customs pipeline.  Passenger throughput is simulated with random service times pulled from triangular distributions.  Passengers are updated and simulation metrics are written back to the database.  Common SQL analysis queries provide separate output files that return optimized schedules, heuristic schedules, and program logs, all in friendly CSV format.
* **Tracking**: A region-of-interest is defined for each potential wave object in which we expect the wave to exist in successive frames.  The wave's representation is captured using simple linear search through the ROI and its dynamics are updated according to center-of-mass measurements.


## Data and Assumptions



## Launching the Model

This project uses

You can launch the program from the command line after navigating to the parent directory and...

joe_bloggs:~/customs$ python customs.py 20

You should see output like this:

...
    

The program will report simple statistics at the conclusion of analysis, like the following:

    End of video reached successfully.
    ...

## Output Files

A log of the tracking routine is written to 



## Model Discussion
