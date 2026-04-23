================
Trips
================

----------------
PopulationGroupDayTrips
----------------

Group-level day plans for a given population can be generated using
the class ``PopulationGroupDayTrips``.

 .. automodule:: mobility.trips.group_day_trips
    :members:

Use ``Parameters`` to change the model configuration.

 .. automodule:: mobility.trips.group_day_trips.core.parameters
    :members:

----------------
Helpers
----------------
``PopulationGroupDayTrips`` produces results through ``RunResults`` and stores
transition events that can be analyzed with the evaluation helpers.

 .. automodule:: mobility.trips.group_day_trips.core.results
    :members:


----------------
IndividualYearTrips
----------------

Individual trips can be sampled using the class ``IndividualYearTrips``.
Use ``.get()`` to access the generated dataframe.

 .. automodule:: mobility.trips.individual_year_trips
    :members:

The trip generation uses the ``safe_sample`` module to ensure we do not use non-representative data
(from a group below the minimal sample size).

 .. automodule:: mobility.trips.individual_year_trips.safe_sample
    :members:
