import smart_open
import boto3
import botocore
import os
import datetime
from dotenv import load_dotenv, find_dotenv
from kafka import KafkaProducer

load_dotenv(find_dotenv())

class RawNOAAWeatherProducer:
    '''
    Producer that takes raw NOAA data from S3 and pushes to Kafka.
    '''
    def __init__(self, s3_bucket):
        '''
        Creates a new Producer.

        :param s3_bucket: Name of S3 bucket with raw data
        '''
        self.s3_bucket = s3_bucket
        self.s3_url = "s3://" + self.s3_bucket + "/"
        self.bootstrap_servers = [i for i in
            os.environ.get("KAFKA_BOOTSTRAP_SERVERS").split(" ")]
        self.producer = KafkaProducer(bootstrap_servers=self.bootstrap_servers,
            key_serializer=lambda v: v.encode('utf-8'),
            value_serializer=lambda v: v.encode('utf-8'))    
    
    def __format(self, data_row):
        '''
        Returns formatted data from a raw line of data from NOAA data dump.

        :param data_row: String, line of raw data
        :returns:        Formatted data, None if required parameters are
                         missing.
        '''
        try:
            USAF = int(data_row[4:10])
            WBAN = int(data_row[10:15])
            obs_date_str = data_row[15:23]
            obs_time_str = data_row[23:27]
            obs_time = datetime.datetime.strptime(obs_date_str + " " + 
                obs_time_str, "%Y%m%d %H%M")
            temp = int(data_row[87:92]) / 10.0
        except:
            return None
        
        if temp > 70 or temp < -70:
            return None
        
        # Check data quality flag for temperature
        try:
            temp_quality = int(data_row[92])
            if (temp_quality == 2 or temp_quality == 3 or temp_quality == 6
                or temp_quality == 7):
                return None
        except:
            return None
        
        return (str(USAF) + "|"
            + str(WBAN) + "|"
            + obs_date_str + "|"
            + obs_time_str + "|"
            + str(temp))

    def send_data(self, objects, topic):
        '''
        Takes in a list of strings, representing the keys for the objects
        from S3 to stream out to Kafka.

        :param objects: List of strings. Each string is a key to an object
                        in the S3 store, for which to produce data from.
        '''
        # TODO: Is sending the raw text from S3 faster, or is serializing the
        # data into something like Avro/ProtoBufs, sending it, and
        # deserializing downstream faster? Where do we win in terms of total
        # throughput?
        s3 = boto3.resource('s3')
        for obj_key in objects:
            try:
                s3.Object(self.s3_bucket, obj_key).load()
            except botocore.exceptions.ClientError as e:
                if e.response['Error']['Code'] == "404":
                    continue
                else:
                    # File does not exist
                    raise Exception("Object '" + obj_key + "' does not exist.")
            for line in smart_open.smart_open(self.s3_url + obj_key):
                data = self.__format(line)
                if data:
                    self.producer.send(topic=topic, value=data)

if __name__ == "__main__":
    producer = RawNOAAWeatherProducer(os.environ.get("S3_BUCKET"))
    producer.send_data(["2017-1.txt"], "raw-noaa-producer")