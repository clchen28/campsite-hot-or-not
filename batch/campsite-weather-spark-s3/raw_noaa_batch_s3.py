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
    :returns:    Int, milliseconds after UNIX epoch
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
    :returns:    Int, time increment from the window top of the hour for the
                 time window that this element belongs in
    '''
    measurement_time = parse_time(data)
    window_time = measurement_time
    if (measurement_time.minute >= 30):
        window_time = window_time + datetime.timedelta(hours=1)
        delta_time = 60 - measurement_time.minute
    else:
        delta_time = measurement_time.minute
    if delta_time == 0:
        delta_time = 0.01
    return float(delta_time)


def map_station_id_to_location(data):
    '''
    Takes raw data from S3 and parses out tuple of weather station location
    :param data: Raw string data from S3
    :returns:    Tuple with values (lat, lon) weather station location data
                 exists, None otherwise
    '''
    location = get_station_location(data)
    lat = float(location.get("lat", None))
    lon = float(location.get("lon", None))
    measurement_time = parse_time(data)
    delta_time = get_dt(data)
    temp = parse_temp(data)
    return {"measurement_time": measurement_time,
        "delta_time": delta_time,
        "lat": lat,
        "lon": lon,
        "temp": temp}

def map_dt_to_weights_and_weightprods(rdd):
    '''
    Takes RDD of temperature data at weather stations, and calculates the time
    weight, as well as the product of the temperature of the time weight and
    the temperature. Useful for downstream process to calculate time-weighted
    temperature mean.

    :param rdd: RDD of temperature data at weather stations
    :returns:   RDD with time weight and time weight * temperature product
    '''
    weight = abs(1 / float(rdd.get("delta_time")))
    rdd["weight"] = weight
    rdd["weight_temp_prod"] = weight * rdd.get("temp")
    return rdd

def closest_hour(rdd):
    '''
    Takes RDD and maps time window to closest hour
    :param rdd: RDD with time window
    :returns:   RDD with window mapped to closest hour
    '''
    print("HERE!")
    for i in rdd:
        print(i)
    new_time = rdd + datetime.timedelta(minutes=30)
    return new_time

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
    # raw_data = sc.textFile(s3_bucket + "2016-1.txt")
    raw_data = sc.textFile("2015-short.txt")

    # Define schema
    schema = StructType([
        StructField("measurement_time", TimestampType(), False),
        StructField("delta_time", FloatType(), False),
        StructField("weight", FloatType(), False),
        StructField("weight_temp_prod", FloatType(), False),
        StructField("temp", FloatType(), False),
        StructField("lat", FloatType(), False),
        StructField("lon", FloatType(), False)
    ])

    # Transform station id's to locations and create DataFrame
    filtered_data = raw_data.map(map_station_id_to_location)\
        .map(map_dt_to_weights_and_weightprods)
    df = spark.createDataFrame(filtered_data, schema)

    # Group measurements into hourly buckets and calculate sum of products
    # of weighted temperature and weights
    df = df.groupBy(window(timeColumn="measurement_time",
            windowDuration="60 minutes",
            startTime="30 minutes"), "lat", "lon")\
        .agg({"weight": "sum", "weight_temp_prod": "sum"})\
        .printSchema()
        #.withColumn("time_weighted_temp", df.sum(weight_temp_prod) / df.sum(weight))\
        

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
