from math import radians, sin, cos, sqrt, asin
import json

def get_json_from_file(filename):
    '''
    Takes text file with one line of JSON and converts to dict

    :param filename: Filename of raw data
    :returns:        Dict with keys of the form USAF|WBAN, and values are dicts
                     with lat and lon keys
    '''
    with open(filename) as f:
        raw_json = f.readline()
    return json.loads(raw_json)

def calc_distance(lat1, lon1, lat2, lon2):
    '''
    Calculates Haversine distance between two lat/lon coordinates
    :param lat1: Latitude of first point
    :param lon1: Longitude of first point
    :param lat2: Latitude of second point
    :param lon2: Longitude of second point
    :returns:    Float, distance between two points in km
    '''
    R = 6372.8 # Earth radius in kilometers

    delta_lat = radians(lat2 - lat1)
    delta_lon = radians(lon2 - lon1)
    lat1 = radians(lat1)
    lat2 = radians(lat2)

    a = sin(delta_lat / 2.0) ** 2 + cos(lat1) * cos(lat2) * sin(delta_lon / 2.0) ** 2
    c = 2 * asin(sqrt(a))

    return R * c

STATION_LOCATIONS = get_json_from_file("stations_latlon.json")
CAMPGROUNDS = get_json_from_file("campgrounds_min.json")
CAMPGROUNDS = CAMPGROUNDS.get("campgrounds")

stations_to_campgrounds = {}

for station in STATION_LOCATIONS.keys():
    lat = float(STATION_LOCATIONS[station]["lat"])
    lon = float(STATION_LOCATIONS[station]["lon"])
    if lat == 0 and lon == 0:
        continue
    stations_to_campgrounds[station] = {"lat": float(STATION_LOCATIONS[station]["lat"]),
        "lon": float(STATION_LOCATIONS[station]["lon"]),
        "nearby_campgrounds": []}
    for campground in CAMPGROUNDS:
        try:
            lat1 = stations_to_campgrounds[station]["lat"]
            lon1 = stations_to_campgrounds[station]["lon"]
            lat2 = float(campground.get("lat"))
            lon2 = float(campground.get("lon"))
        except:
            continue
        if int(lat2) == 0 and int(lon2) == 0:
            continue
        if calc_distance(lat1, lon1, lat2, lon2) <= 65:
            stations_to_campgrounds[station]["nearby_campgrounds"].append(campground)

with open("stations_to_nearby_campgrounds.json", "w") as f:
    f.write(json.dumps(stations_to_campgrounds))