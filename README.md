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

Main() implements the recognition workflow from above. The following bullets list the modeling operation employed in this program, and a full discussion on model choices can be found in ["Model Details"](##ModelDetails) below.

* **Preprocessing**: Input frames are downsized by a factor of four for analysis.  Background modeling is performed using a Mixture-of-Gaussians model with five Gaussians per pixels and a background history of 300 frames, resulting in a binary image in which background is represented by values of 255 and foreground as 0.  A square denoising kernel of 5x5 pixels is applied pixel-wise to the binary image to remove foreground features that are too small to be considered objects of interest.
* **Detection**: Contour-finding is applied to the denoised image to identify all forground objects.  These contours are filtered for both area and shape using a contour's moments, resulting in the return of large, oblong shapes in the scene.  These contours are converted to Wave objects and passed to the tracking routine.
* **Tracking**: A region-of-interest is defined for each potential wave object in which we expect the wave to exist in successive frames.  The wave's representation is captured using simple linear search through the ROI and its dynamics are updated according to center-of-mass measurements.
* **Recognition**: We use two dynamics to determine whether or not the tracked object is indeed a positive instance of a wave: mass and displacement.  Mass is calculated by weighting pixels equally and performing a simple count.  Displacement is measured by calculating the orthogonal displacement of the wave's center-of-mass relative to its original major axis.  We accept an object as a true instance of a wave if its mass and orthogonal displacement exceed user-defined thresholds.


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
