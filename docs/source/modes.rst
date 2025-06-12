================
Transport modes
================

The mobility behavior of a population depends in part on the perceived cost of the different modes of transport that are available. The perceived cost of a given trip with a given mode aggregates factors in its cost of distance (€/km), its cost of time (€/h), and a cost constant (€/trip). We can then estimate how likely it is that a person chooses some mode instead of another, by comparing their perceived costs.

To be able to compute these perceived costs, Mobility estimates trips distances and travel times between all possible origin * destinations pairs for all transport zones under study (with a travel time threshold so we avoid computing unlikely trips such as 3 hour walks).

The available modes of transports that can be used in Mobility are : 

* Unimodal modes:

    * Walk.
    * Bicycle.
    * Car.

* Multimodal modes:

    * Carpool:

        * Access to the carpool pick up point : car, single occupant.
        * Carpool ride : car, multiple occupants.

    * Public transport:

        * Access : any of the available unimodal modes. 
        * Public transport ride : any combination of public transport services (bus, tramway, subway...).
        * Egress : any of the available unimodal modes (usually walk). 

---------------
Unimodal modes
---------------

^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
OpenStreetMap Data Preparation
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The geographic data describing road, cycling, and pedestrian networks is sourced from OpenStreetMap. Maximum speeds are inferred from the road category when not explicitly provided in the data.

^^^^^^^^^^^^^^^^^^^^^^^^
Transport Graph Creation
^^^^^^^^^^^^^^^^^^^^^^^^

OpenStreetMap data is converted into a transport graph and simplified to accelerate computations:

* **Successive road segments without intersections are merged** by summing their distances and travel times, and averaging their capacities and congestion parameters.

* **Dead-end segments** are removed. It is assumed that buildings representing transport zones are located near roads that are not dead ends, and that dead ends will never be used for routing between transport zones.

The following table indicates which OSM road categories are retained for each of the three modes:

===================  ====  =====  =====    
OSM Category         Car   Walk   Bike 
===================  ====  =====  =====
bridleway            No    Yes    Yes  
cycleway             No    Yes    Yes  
ferry                Yes   Yes    Yes  
footway              No    Yes    Yes  
living_street        Yes   Yes    Yes  
motorway             Yes   No     No   
motorway_link        Yes   No     No   
path                 No    Yes    Yes  
pedestrian           No    Yes    Yes  
primary              Yes   Yes    Yes  
primary_link         Yes   Yes    Yes  
residential          Yes   Yes    Yes  
secondary            Yes   Yes    Yes  
secondary_link       Yes   Yes    Yes  
service              Yes   Yes    Yes  
steps                No    Yes    Yes  
tertiary             Yes   Yes    Yes  
tertiary_link        Yes   Yes    Yes  
track                No    Yes    Yes  
trunk                Yes   Yes    Yes  
trunk_link           Yes   Yes    Yes  
unclassified         Yes   Yes    Yes  
===================  ====  =====  =====


----------------
Public transport
----------------

^^^^^^^^^^^^^^^^^^^^^
GTFS Data Preparation
^^^^^^^^^^^^^^^^^^^^^

The data detailing public transport service is sourced from feeds published in the GTFS (General Transit Feed Specification) standard: stop locations, days and times of service, and transport modes (bus, tram, train, metro, etc.).

Operator feeds are filtered to retain only lines with at least one stop in the studied transport zones. They are then aligned to a common date before being merged into a single feed.

Transfers between lines that were not originally specified are added within a 200-meter radius around each stop, with transfer times calculated based on straight-line distance between stops.

The public transport service from the Tuesday with the most operational services is used as the reference for modeling.

The GTFS data versions used for the Greater Geneva study are from late 2024.

^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Public Transport Graph Creation
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The public transport offer is converted into an equivalent transport graph, allowing the construction of multimodal graphs by combining it with access and egress mode graphs (walking, biking, driving):

* **Creation of entry and exit nodes** in the public transport network by grouping nearby stops (within 40 meters) to reduce their number.

* **Computation of average travel times** between two stops on the same line, and of **waiting times** between arrivals and departures of the same line at a stop.

* **Computation of average minimum transfer times** between each arrival and the next departures of the other accessible services. Transfers exceeding 20 minutes are discarded.

* **Calculation of initial waiting time**, based on average headway: 50% of the headway if under 10 minutes, otherwise 5 minutes.

* **Calculation of perceived waiting time**, modeling the risk that the expected service is missed and the user must wait for the next one. This is derived from the line's average headway and a perceived probability of missed service.

* **Calculation of arrival time differences**, between the target arrival time and all possible actual arrival times at the destination stop.

* **Calculation of straight-line distances between stops**, used for distance-based metrics.

The resulting travel times are therefore approximations of actual travel times. In reality, the **departure time** impacts travel duration, as several factors vary throughout the day: transfer times, vehicle speeds, etc.

^^^^^^^^^^^
Travel Time
^^^^^^^^^^^

The total travel time is composed of:

* A **waiting time at the access node** of departure (precautionary time).

* (Optional) A **travel time to an intermediate stop** for a transfer.

* (Optional) A **waiting time for the transfer**.

* A **travel time to the egress node**.

The number of transfers is **not limited** by the model, but a **maximum travel time** is enforced to exclude unlikely trips.


---------
Functions
---------

You must describe the transport modes that you want to model. Most usual modes are available, and you can use a combination of any mode with public transport.
Available modes : walk, bicycle, car, carpool, public transport (+any mode before or after using public transport).

 .. automodule:: mobility.transport_modes
    :members:
