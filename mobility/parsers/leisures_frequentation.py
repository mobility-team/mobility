# Mapping of messy OSM leisure values to clean, canonical OSM leisure tags.
# Only values that require correction or special handling are included here.

LEISURE_MAPPING = {
    # French variants / lexical variants
    "parc": "park",
    "citypark": "park",
    "centre_de_loisirs": "sports_centre",
    "terrain de boules": "miniature_golf",

    # Spelling variations and common typos
    "pingpong": "table_tennis_table",
    "sport_center": "sports_centre",
    "sport_centre": "sports_centre",
    "sport_hall": "sports_hall",
    "lake_bath": "bathing_place",
    "flussbad": "bathing_place",

    # Combined values
    "miniature_golf;trampoline_park;sports_centre": "miniature_golf",
    "sports_centre;pitch": "sports_centre",
    "sports_centre;jump_park": "sports_centre",
    "swimming_pool;ice_rink": "swimming_pool",
    "swimming_pool;sports_centre": "swimming_pool",

    # Escape game variants
    "laser_game": "escape_game",
    "lasertag": "escape_game",
    "escape game": "escape_game",

    # Spa / sauna / wellness
    "spa": "sauna",
    "healthspa": "sauna",
    "thalasso": "sauna",
    "thalassotherapy": "sauna",

    # Out-of-scope or useless values → removed
    "building": None,
    "parking": None,
    "forest": None,
    "footway": None,
    "construction": None,
    "vacant": None,
    "proposed": None,
    "natural": None,
    "garss": None,
    "grass": None,
    "dr": None,
    "fes": None,
    "check": None,
    "spot": None,
    "island": None,
    "refuge": None,
    "detention": None,
    "hostel": None,
    "tourism": None,
    "boat": None,
    "coworking_space": None,
    "association": None,
    "music": None,
}


# Frequency scores for clean leisure categories
# 4 = high footfall, 1 = low footfall
LEISURE_FREQUENCY = {
    "stadium": 4,
    "sports_centre": 4,
    "sports_hall": 4,
    "swimming_pool": 4,
    "water_park": 4,
    "amusement_arcade": 12,
    "adult_gaming_centre": 4,
    "escape_game": 4,
    "theme_park": 4,

    "park": 3,
    "garden": 3,
    "playground": 3,
    "miniature_golf": 3,
    "golf_course": 3,
    "marina": 3,
    "fitness_centre": 3,
    "fitness_station": 3,
    "ice_rink": 3,
    "trampoline_park": 3,
    "bathing_place": 3,

    "recreation_ground": 2,
    "picnic": 2,
    "picnic_table": 2,
    "bird_hide": 2,
    "wildlife_hide": 2,
    "table_tennis_table": 2,
    "horse_riding": 2,
    "fishing": 2,
    "community_centre": 2,
    "social_club": 2,
    "summer_camp": 2,
    "schoolyard": 2,
    "bandstand": 2,
    "dance": 2,

    "sauna": 1,
    "turkish_bath": 1,
    "common": 1,
    "village_green": 1,
    "yes": 1,
}
