import sys
import json
import configparser
import datetime
from pytz import timezone
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SparkSession

# FIXME: For invalid data, should fail gracefully and simply not process that value

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
    :returns:    Int, UNIX timestamp
    '''
    raw_date_time = data[15:23] + ' ' + data[23:27]
    date_time = datetime.datetime.strptime(raw_date_time, "%Y%m%d %H%M")
    unix_time = date_time.replace(tzinfo=timezone('UTC')).timestamp()
    return int(unix_time)

def parse_temp(data):
    '''
    Takes raw data from S3 and parses out temperature reading
    :param data: Raw string data from S3
    :returns:    Float of temperature reading
    '''
    return float(data[87:92]) / 10.0

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
    event_time = parse_time(data)
    temp = parse_temp(data)

    return {"event_time": event_time, "lat": lat, "lon": lon, "temp": temp}

if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read("s3_spark.cfg")

    # Make dict of station locations available to all nodes
    STATION_LOCATIONS = get_station_locations_from_file("stations_latlon.json")

    # SparkContext represents entry point to Spark cluster
    # Automatically determines master
    sc = SparkContext(appName="LocationStreamConsumer")
    spark = SparkSession(sc)
    s3_bucket = config.get("s3", "bucket_url")

    # TODO: Don't hardcode one object
    # Returns an RDD of strings
    raw_data = sc.textFile(s3_bucket + "2016-1.txt")
    
    # Transform station id's to locations
    raw_data.map(map_station_id_to_location).toDF().foreach(print)