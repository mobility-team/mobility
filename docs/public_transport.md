# Public transport mode
## GTFS Data Preparation
The data detailing public transport service is sourced from feeds published in the GTFS (General Transit Feed Specification) standard: stop locations, days and times of service, and transport modes (bus, tram, train, metro, etc.).

Operator feeds are filtered to retain only lines with at least one stop in the studied transport zones. They are then aligned to a common date before being merged into a single feed.

Transfers between lines that were not originally specified are added within a 200-meter radius around each stop, with transfer times calculated based on straight-line distance between stops.

The public transport service from the Tuesday with the most operational services is used as the reference for modeling.

The GTFS data versions used for this Greater Geneva study are from late 2024.

## Public Transport Graph Creation
The public transport offer is converted into an equivalent transport graph, allowing the construction of multimodal graphs by combining it with access and egress mode graphs (walking, biking, driving):
- **Creation of entry and exit nodes** in the public transport network by grouping nearby stops (within 40 meters) to reduce their number.
- **Computation of average travel times** between two stops on the same line, and of **waiting times** between arrivals and departures of the same line at a stop.
- **Computation of average minimum transfer times** between each arrival and the next departures of the other accessible services. Transfers exceeding 20 minutes are discarded.
- **Calculation of initial waiting time**, based on average headway: 50% of the headway if under 10 minutes, otherwise 5 minutes.
- **Calculation of perceived waiting time**, modeling the risk that the expected service is missed and the user must wait for the next one. This is derived from the line's average headway and a perceived probability of missed service.
- **Calculation of arrival time differences**, between the target arrival time and all possible actual arrival times at the destination stop.
- **Calculation of straight-line distances between stops**, used for distance-based metrics.

The resulting travel times are therefore approximations of actual travel times. In reality, the **departure time** impacts travel duration, as several factors vary throughout the day: transfer times, vehicle speeds, etc.

## Travel Time
The total travel time is composed of:
- A **waiting time at the access node** of departure (precautionary time).
- (Optional) A **travel time to an intermediate stop** for a transfer.
- (Optional) A **waiting time for the transfer**.
- A **travel time to the egress node**.

The number of transfers is **not limited** by the model, but a **maximum travel time** is enforced to exclude unlikely trips.
