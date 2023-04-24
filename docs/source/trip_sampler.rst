================
Trip sampler
================

The trip sampler uses data from the surveys to sample trips for a given list of persons,
using the ``get_trips`` method.

You can find an example of this model in <https://github.com/mobility-team/mobility/tree/main/examples/Millau>.

 .. automodule:: mobility.trip_sampler
    :members:

The trip sampler uses the ``safe_sample`` module to ensure we do not use non-reprentative data
(from a group below the minimal sample size).

 .. automodule:: mobility.safe_sample
    :members:

The surveys data (from INSEE) is retrieved thanks to the get_survey_data module.

 .. automodule:: mobility.get_survey_data
    :members:

It is completed by the get_insee_data module for other INSEE data.

 .. automodule:: mobility.get_insee_data
    :members:
