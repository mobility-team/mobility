================
Trips
================

Trips can be sampled using the module Trips. Use the .get() method to access a dataframe with those trips.

 .. automodule:: mobility.trips
    :members:

The trip generation uses the ``safe_sample`` module to ensure we do not use non-representative data
(from a group below the minimal sample size).

 .. automodule:: mobility.safe_sample
    :members:
