import sys
import json
import configparser
import datetime
from math import radians, sin, cos, sqrt, asin
from pytz import timezone
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import (StructType, StructField, FloatType,
                               TimestampType, IntegerType, StringType)

def get_station_locations_from_file(filename):
    '''
    Takes text file with one line of JSON, containing raw data from NOAA,
    which maps weather station id's to latitude and longitude, and returns
    a dict of this data.

    :param filename: Filename of raw data
    :returns:        Dict with keys of the form USAF|WBAN, and values are dicts
                     with lat and lon keys
    '''
    with open(filename) as f:
        raw_json = f.readline()
    return json.loads(raw_json)

def parse_USAF(data):
    '''
    Takes raw data from S3 and parses out USAF
    :param data: Raw string data from S3
    :returns:    String of USAF id number
    '''
    return data[4:10]

def parse_WBAN(data):
    '''
    Takes raw data from S3 and parses out WBAN
    :param data: Raw string data from S3
    :returns:    String of WBAN id number
    '''
    return data[10:15]

def parse_time(data):
    '''
    Takes raw data from S3 and parses out observation time
    :param data: Raw string data from S3
    :returns:    Datetime object of raw string
    '''
    raw_date_time = data[15:23] + ' ' + data[23:27]
    try:
        date_time = datetime.datetime.strptime(raw_date_time, "%Y%m%d %H%M")\
            .replace(tzinfo=timezone('UTC'))
    except:
        return None
    return date_time

def parse_temp(data):
    '''
    Takes raw data from S3 and parses out temperature reading
    :param data: Raw string data from S3
    :returns:    Float of temperature reading
    '''
    raw_temp = data[87:92]
    if raw_temp == "+9999":
        return None
    try:
        temp = float(raw_temp) / 10.0
    except:
        return None
    if temp == 999.9:
        return None
    return temp

def get_station_location(data):
    '''
    Takes raw data from S3 and parses out tuple of weather station location
    :param data: Raw string data from S3
    :returns:    Tuple with values (lat, lon) weather station location data
                 exists, None otherwise
    '''
    USAF = parse_USAF(data)
    WBAN = parse_WBAN(data)
    return STATION_LOCATIONS.get(USAF + "|" + WBAN, None)

def get_dt(data):
    '''
    Return the appropriate window time, i.e.,. the time at the middle of the
    time window that this data set belongs in. Measurements at each weather
    station are grouped into hourly windows, and time-weighted averages are
    calculated using data that are half an hour apart from a given time.
    :param data: Raw string data from S3
    :returns:    Tuple, first element is the time of the closest hour, second
                 element is delta time from the closest hour
    '''
    measurement_time = parse_time(data)
    closest_hour = measurement_time
    if (measurement_time.minute >= 30):
        closest_hour = closest_hour + datetime.timedelta(hours=1)
        delta_time = 60 - measurement_time.minute
    else:
        delta_time = measurement_time.minute
    if delta_time == 0:
        delta_time = 0.01
    closest_hour = closest_hour.replace(minute=0)
    return (closest_hour, float(delta_time))

def dt_to_weights_and_weightprods(dt, temp):
    '''
    Takes RDD of temperature data at weather stations, and calculates the time
    weight, as well as the product of the temperature of the time weight and
    the temperature. Useful for downstream process to calculate time-weighted
    temperature mean.

    :param dt:  Float of delta time at a weather station
    :returns:   Tuple, first element is weight, second element is product of
                weight and temperature reading
    '''
    weight = abs(1 / float(dt))
    weight_temp_prod = weight * temp
    return (weight, weight_temp_prod)

def map_raw_to_station_measurements(data):
    '''
    Takes raw data from S3 and parses out tuple of weather station location
    :param data: Raw string data from S3
    :returns:    Dict with values corresponding to weather measurement info
    '''
    location = get_station_location(data)
    if not location:
        return []
    USAF = parse_USAF(data)
    WBAN = parse_WBAN(data)
    lat = float(location.get("lat", None))
    lon = float(location.get("lon", None))
    measurement_time = parse_time(data)
    closest_hour, delta_time = get_dt(data)
    temp = parse_temp(data)
    if not temp:
        return []
    weight, weight_temp_prod = dt_to_weights_and_weightprods(delta_time, temp)
    return [(closest_hour, lat, lon, USAF + "|" + WBAN, weight_temp_prod, weight)]

def sum_weight_and_prods(val1, val2):
    '''
    Given a Key-Value RDD, sums the weights and weight*temp products
    :param val1: Value of first RDD
    :param val2: Value of second RDD
    :returns:    RDD with tuples reduced based on weights and weight*temp
    '''
    return (val1[0] + val2[0], val1[1] + val2[1])

def calc_weighted_average_station(rdd):
    '''
    Given that the first value of the RDD is the sum of the weighted temps
    and that the second is the sum of the weights, calculates the
    weighted average and returns RDD with just this as the value.
    :param rdd: RDD that contains weights and weighted temps
    :returns:   RDD with value as the weighted average temp
    '''
    weighted_avg = rdd[1][0] / float(rdd[1][1])
    measurement_hour = rdd[0][0]
    lat = float(rdd[0][1])
    lon = float(rdd[0][2])
    station_id = rdd[0][3]
    return (measurement_hour, lat, lon, station_id, weighted_avg)

def calc_weighted_average_campsite(rdd):
    '''
    Given that the first value of the RDD is the sum of the weighted temps
    and that the second is the sum of the weights, calculates the
    weighted average and returns RDD with just this as the value.
    :param rdd: RDD that contains weights and weighted temps
    :returns:   RDD with value as the weighted average temp
    '''
    weighted_avg = rdd[1][0] / float(rdd[1][1])
    measurement_hour = rdd[0][0]
    lat = float(rdd[0][1])
    lon = float(rdd[0][2])
    campsite_id = rdd[0][3]
    campsite_name = rdd[0][4]
    return (measurement_hour, lat, lon, campsite_id, campsite_name, weighted_avg)

def calc_distance(lat1, lon1, lat2, lon2):
    '''
    Calculates Haversine distance between two lat/lon coordinates
    From https://rosettacode.org/wiki/Haversine_formula#Python
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

def station_to_campsite(rdd):
    '''
    Takes RDD with weather station reading and measurement time, and returns
    several RDDs for readings transformed to nearest campsites.
    :param rdd: RDD of weather station readings
    :returns:   RDDs of weather station reading transformed to nearest
                nearest campsites. Returns empty RDD if there are no
                nearby campsites.
    '''
    # Returns a list of nearby campsites
    # Each campsite has keys of lat, lon, facilityId, legacyFacilityId, and name
    campsites = STATION_LOCATIONS.get(rdd[3]).get("nearby_campgrounds")
    if len(campsites) == 0:
        return []
    measurement_hour = rdd[0]
    station_lat = float(rdd[1])
    station_lon = float(rdd[2])
    station_id = rdd[3]
    temperature = rdd[4]
    measurements = []
    for campsite in campsites:
        campsite_lat = float(campsite.get("lat", None))
        campsite_lon = float(campsite.get("lon", None))
        distance = calc_distance(station_lat, station_lon, campsite_lat, campsite_lon)
        campsite_name = campsite.get("name", None)
        campsite_id = campsite.get("facilityId", None)
        weight = 1 / (float(distance) ** 2)
        weight_temp_prod = temperature * float(weight)
        measurements.append(((measurement_hour, campsite_lat, campsite_lon, campsite_id, campsite_name),
            (weight_temp_prod, weight)))
    return measurements

if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read("s3_spark.cfg")

    # Make dict of station locations available to all nodes
    STATION_LOCATIONS = get_station_locations_from_file("stations_to_nearby_campgrounds.json")

    # SparkContext represents entry point to Spark cluster
    # Automatically determines master
    sc = SparkContext(appName="LocationStreamConsumer")
    spark = SparkSession(sc)
    conf = SparkConf().set("spark.cassandra.connection.host",
        config.get("cassandra_cluster", "host"))

    s3_bucket = config.get("s3", "bucket_url")
    s3_object = config.get("s3", "object")

    # Returns an RDD of strings
    raw_data = sc.textFile(s3_bucket + s3_object)

    # Define schema
    campsite_schema = StructType([
        StructField("calculation_time", TimestampType(), False),
        StructField("lat", FloatType(), False),
        StructField("lon", FloatType(), False),
        StructField("campsite_id", IntegerType(), False),
        StructField("name", StringType(), False),
        StructField("temp", FloatType(), False)
    ])

    station_schema = StructType([
        StructField("measurement_time", TimestampType(), False),
        StructField("lat", FloatType(), False),
        StructField("lon", FloatType(), False),
        StructField("station_id", StringType(), False),
        StructField("weight_temp_prod", FloatType(), False),
        StructField("weight", FloatType(), False)
    ])

    # Transform station id's to locations
    df_data = raw_data.flatMap(map_raw_to_station_measurements)

    # Calculate time weighted average, then flatten
    station_save_options = {"table": "readings",
        "keyspace": "weather_stations"}
    time_weighted_temp = spark\
        .createDataFrame(df_data, station_schema)\
        .repartition(96, "station_id")\
        .groupBy("station_id", "lat", "lon", "measurement_time")\
        .agg(F.sum("weight_temp_prod").alias("weight_temp_prod_sum"),
            F.sum("weight").alias("weight_sum"))\
        .withColumn("temp", (F.col("weight_temp_prod_sum") /
            F.col("weight_sum")))\
        .select(F.col("station_id"), F.col("measurement_time"), F.col("lat"), F.col("lon"), F.col("temp"))\
        .write\
        .format("org.apache.spark.sql.cassandra")\
        .mode('append')\
        .options(**station_save_options)\
        .save()

    """
    station_save_options = {"table": "readings",
        "keyspace": "weather_stations"}
    stations_df = spark.createDataFrame(time_weighted_temp, station_schema)\
        .sort("station_id")\
        .write\
        .format("org.apache.spark.sql.cassandra")\
        .mode('append')\
        .options(**station_save_options)\
        .save()
    """
    
    # Convert time-averaged station measurements to distance-weighted averages
    # at campsites
    """
    campsites_rdd = time_weighted_temp.flatMap(station_to_campsite)\
        .reduceByKey(sum_weight_and_prods)\
        .map(calc_weighted_average_campsite)
    
    campsite_save_options = {"table": "calculations",
        "keyspace": "campsites"}
    campsites_df = spark.createDataFrame(campsites_rdd, campsite_schema)\
        .sort("campsite_id")\
        .write\
        .format("org.apache.spark.sql.cassandra")\
        .mode('append')\
        .options(**campsite_save_options)\
        .save()
    """
