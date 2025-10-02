================
Trips
================

Trips for a given population can be generated using the class ``population_trips``.

 .. automodule:: mobility.choice_models.population_trips
    :members:


Besides population_trips, trips can be sampled using the class ``trips``. Use the .get() method to access a dataframe with those trips.

 .. automodule:: mobility.trips
    :members:

The trip generation uses the ``safe_sample`` module to ensure we do not use non-representative data
(from a group below the minimal sample size).

 .. automodule:: mobility.safe_sample
    :members:
