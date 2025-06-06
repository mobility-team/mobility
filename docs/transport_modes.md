# Transport modes
The mobility behavior of a population depends in part on the perceived cost of the different modes of transport that are available. The perceived cost of a given trip with a given mode aggregates factors in its cost of distance (€/km), its cost of time (€/h), and a cost constant (€/trip). We can then estimate how likely it is that a person chooses some mode instead of another, by comparing their perceived costs.

To be able to compute these perceived costs, Mobility estimates trips distances and travel times between all possible origin - destinations pairs for all transport zones under study (with a travel time threshold so we avoid computing unlikely trips such as 3 hour walks).

The available modes of transports that can be used in Mobility are : 
- [Unimodal modes](./unimodal_modes.md) :
    - Walk.
    - Bicycle.
    - Car.
- Multimodal modes :
    - Carpool :
        - Access to the carpool pick up point : car, single occupant.
        - Carpool ride : car, multiple occupants.
    - [Public transport](./public_transport.md) :
        - Access : any of the available unimodal modes. 
        - Public transport ride : any combination of public transport services (bus, tramway, subway...).
        - Egress : any of the available unimodal modes (usually walk). 

