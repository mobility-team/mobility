# Current countries

Mobility has built-in data preparation for France and Switzerland.

The tables below list the data sources used by the package today. They are a
reference for modellers adding another country.

## France

| Input | Source used today | Prepared in | Notes |
| --- | --- | --- | --- |
| Local admin units | IGN ADMIN EXPRESS COG CARTO | `FrenchAdminUnits` | Communes and municipal arrondissements. |
| Local admin unit categories | INSEE urban units | `FrenchLocalAdminUnitsCategories` | Converted to Mobility urban categories. |
| Legal population | INSEE legal population | `FrenchCityLegalPopulation` | Prepared by local admin unit. |
| Population groups | INSEE census localized individuals | `FrenchPopulationGroups` | Joined to regions, cantons, and transport zones. |
| Mobility survey | EMP mobility survey | project survey object | Used for daily activity, time, and mode patterns. |
| Work opportunities | INSEE 2019 jobs and active population | `FrenchWorkOpportunities` | Jobs and active population by local admin unit. |
| Work flows | INSEE MOBPRO 2019 | `FrenchWorkFlows` | Home-work flows by local admin unit and mode. |
| Study opportunities | French education directory and 2023 higher education atlas | `FrenchStudyOpportunities` | Schools and universities as point opportunities. |
| Study flows | INSEE MOBSCO 2020 | `FrenchStudyFlows` | Home-study flows by local admin unit and school type. |
| Shopping opportunities | INSEE BPE 2024 and ESANE turnover ratios | `FrenchShoppingOpportunities` | Shop proxy based on equipment and turnover ratios. |
| Public transport | transport.data.gouv.fr GTFS metadata | `FrenchGTFS` | Filtered by the selected study area when possible. |

## Switzerland

| Input | Source used today | Prepared in | Notes |
| --- | --- | --- | --- |
| Local admin units | swisstopo swissBOUNDARIES3D | `SwissAdminUnits` | Municipalities. |
| Local admin unit categories | BFS municipality typology 2020 | `SwissLocalAdminUnitsCategories` | Converted to Mobility urban categories. |
| Legal population | BFS municipal population table | `SwissCityLegalPopulation` | Prepared by local admin unit. |
| Population groups | User-provided Swiss census parser | `SwissPopulationGroups` | Required when Swiss transport zones are present. |
| Mobility survey | Project survey object | project survey object | Must have `survey_name` and `country`. |
| Work opportunities | BFS jobs and active population table | `SwissWorkOpportunities` | Jobs and active population by local admin unit. |
| Work flows | BFS commuting table | `SwissWorkFlows` | Home-work flows by local admin unit. |
| Study opportunities | OpenStreetMap schools and Swiss student totals for 2023/24 and 2024/25 | `SwissStudyOpportunities` | Estimated from school building area. |
| Study flows | Missing | `SwissStudyFlows` | Mobility fails clearly if Swiss study flows are requested. |
| Shopping opportunities | BFS STATENT 2022 and INSEE/NAF bridge | `SwissShoppingOpportunities` | Shop proxy based on employees and turnover ratios. |
| Public transport | opentransportdata.swiss GTFS metadata | `SwissGTFS` | Uses Swiss national GTFS source metadata. |
