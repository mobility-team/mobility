================
Trips
================

----------------
PopulationTrips
----------------

Trips for a given population can be generated using the class ``population_trips``.

 .. automodule:: mobility.choice_models.population_trips
    :members:

Use ``PopulationTripsParameters`` to change the parameters.

 .. automodule:: mobility.choice_models.population_trips_parameters
    :members:

``population_trips`` uses different other classes :
# ``StateInitializer``: initialises the data
# ``DestinationSequenceSampler`` samples destinations sequences
# ``TopKModeSequenceSearch`` finds the k best modal chains for each destination sequence
# ``StateUpdater`` updates population state distributions over motive/destination/mode sequence

 .. automodule:: mobility.choice_models.state_initializer
    :members:

 .. automodule:: mobility.choice_models.destination_sequence_sampler
    :members:

 .. automodule:: mobility.choice_models.top_k_mode_sequence_search
    :members:

 .. automodule:: mobility.choice_models.state_updater
    :members:


----------------
Helpers
----------------
``TopKModeSequenceSearch`` uses the function ``add_index`` to ensure a stable integer index exists for a categorical or string column.


 .. automodule:: mobility.choice_models.add_index
    :members:


----------------
Trips
----------------

Besides population_trips, trips can be sampled using the class ``Trips``. Use the .get() method to access a dataframe with those trips.

 .. automodule:: mobility.trips
    :members:

The trip generation uses the ``safe_sample`` module to ensure we do not use non-representative data
(from a group below the minimal sample size).

 .. automodule:: mobility.safe_sample
    :members:
