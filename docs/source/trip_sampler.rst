================
Trip sampler
================

The trip sampler uses data from the surveys to sample trips for a given list of persons,
using the ``get_trips`` method.

You can find an example of this model in <https://github.com/mobility-team/mobility/tree/main/examples/Millau>.

 .. automodule:: mobility.trips
    :members:

The trip sampler uses the ``safe_sample`` module to ensure we do not use non-reprentative data
(from a group below the minimal sample size).

 .. automodule:: mobility.safe_sample
    :members:

