# Unimodal modes
## OpenStreetMap Data Preparation
The geographic data describing road, cycling, and pedestrian networks is sourced from OpenStreetMap. Maximum speeds are inferred from the road category when not explicitly provided in the data.

## Transport Graph Creation
OpenStreetMap data is converted into a transport graph and simplified to accelerate computations:
- **Successive road segments without intersections are merged** by summing their distances and travel times, and averaging their capacities and congestion parameters.
- **Dead-end segments** are removed. It is assumed that buildings representing transport zones are located near roads that are not dead ends, and that dead ends will never be used for routing between transport zones.

The following table indicates which OSM road categories are retained for each of the three modes:

| OSM Category       | Car | Walk | Bike |
|--------------------|:---:|:----:|:----:|
| bridleway          | No  | Yes  | Yes  |
| cycleway           | No  | Yes  | Yes  |
| ferry              | Yes | Yes  | Yes  |
| footway            | No  | Yes  | Yes  |
| living_street      | Yes | Yes  | Yes  |
| motorway           | Yes | No   | No   |
| motorway_link      | Yes | No   | No   |
| path               | No  | Yes  | Yes  |
| pedestrian         | No  | Yes  | Yes  |
| primary            | Yes | Yes  | Yes  |
| primary_link       | Yes | Yes  | Yes  |
| residential        | Yes | Yes  | Yes  |
| secondary          | Yes | Yes  | Yes  |
| secondary_link     | Yes | Yes  | Yes  |
| service            | Yes | Yes  | Yes  |
| steps              | No  | Yes  | Yes  |
| tertiary           | Yes | Yes  | Yes  |
| tertiary_link      | Yes | Yes  | Yes  |
| track              | No  | Yes  | Yes  |
| trunk              | Yes | Yes  | Yes  |
| trunk_link         | Yes | Yes  | Yes  |
| unclassified       | Yes | Yes  | Yes  |
