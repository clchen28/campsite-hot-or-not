import sys
import json
import configparser
import datetime
from pytz import timezone
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import window, sum
from pyspark.sql.types import (StructType, StructField, FloatType,
                               TimestampType, IntegerType)

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
    try:
        temp = float(data[87:92]) / 10.0
    except:
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
    lat = float(location.get("lat", None))
    lon = float(location.get("lon", None))
    measurement_time = parse_time(data)
    closest_hour, delta_time = get_dt(data)
    temp = parse_temp(data)
    weight, weight_temp_prod = dt_to_weights_and_weightprods(delta_time, temp)
    return ((closest_hour, lat, lon),
        (weight_temp_prod, weight))

def sum_weight_and_prods(val1, val2):
    '''
    Given a Key-Value RDD, sums the weights and weight*temp products
    :param val1: Value of first RDD
    :param val2: Value of second RDD
    :returns:    RDD with tuples reduced based on weights and weight*temp
    '''
    return (val1[0] + val2[0], val1[1] + val2[1])

def calc_weighted_average(rdd):
    '''
    Given that the first value of the RDD is the sum of the weighted temps
    and that the second is the sum of the weights, calculates the
    weighted average and returns RDD with just this as the value.
    :param rdd: RDD that contains weights and weighted temps
    :returns:   RDD with value as the weighted average temp
    '''
    weighted_avg = rdd[1][0] / float(rdd[1][1])
    measurement_hour = rdd[0][0]
    lat = rdd[0][1]
    lon = rdd[0][2]
    return (measurement_hour, lat, lon, weighted_avg)

if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read("s3_spark.cfg")

    # Make dict of station locations available to all nodes
    STATION_LOCATIONS = get_station_locations_from_file("stations_latlon.json")

    # SparkContext represents entry point to Spark cluster
    # Automatically determines master
    sc = SparkContext(appName="LocationStreamConsumer")
    spark = SparkSession(sc)
    conf = SparkConf().set("spark.cassandra.connection.host",
        config.get("cassandra_cluster", "host"))
    s3_bucket = config.get("s3", "bucket_url")

    # TODO: Don't hardcode one object
    # Returns an RDD of strings
    raw_data = sc.textFile(s3_bucket + "2016-1.txt")
    # raw_data = sc.textFile("2015-short.txt")

    # Define schema
    schema = StructType([
        StructField("measurement_hour", TimestampType(), False),
        StructField("lat", FloatType(), False),
        StructField("lon", FloatType(), False),
        StructField("weighted_avg", FloatType(), False)
    ])

    # Transform station id's to locations
    # Key is a tuple of time, lat, lon
    # Value is a tuple of time weights, and products of time weights and temp
    rdd_data = raw_data.map(map_raw_to_station_measurements)

    # Calculate time weighted average, then flatten to a 
    time_weighted_temp = rdd_data\
        .reduceByKey(sum_weight_and_prods)\
        .map(calc_weighted_average)\
        .foreach(print)

    # Convert to DataFrame and save to cassandra

    df = spark.createDataFrame(filtered_data, schema)
        

    # Calculate time-weighted average temperatures and closest hour
    # df.select("window").rdd.map(closest_hour).toDF()

    # Group measurements into hourly buckets
    # filtered_data.groupBy(window("measurement_time", "30 minutes")).show(30)

    # TODO: Remember to multiply by 1000 again when inserting into cassandra
    '''
    .write\
    .format("org.apache.spark.sql.cassandra")\
    .mode('append')\
    .options(table="readings", keyspace="weather_stations")\
    .save()
    '''
