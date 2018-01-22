CREATE KEYSPACE IF NOT EXISTS weather_stations WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 2 };

CREATE KEYSPACE IF NOT EXISTS campsites WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 2 };

CREATE TABLE weather_stations.readings (
    station_id string,
    year int,
    measurement_time timestamp,
    lat float,
    lon float,
    temp float,
    PRIMARY KEY ((station_id, year), measurement_time)
) WITH CLUSTERING ORDER BY (measurement_time DESC);

CREATE TABLE campsites.calculations (
	campsite_id int,
	year int,
	calculation_time timestamp,
	lat float,
	lon float,
	temp float,
	PRIMARY KEY ((campsite_id, year), calculation_time)
) WITH CLUSTERING ORDER BY (calculation_time DESC);

CREATE CUSTOM INDEX weather_readings_index ON weather_stations.readings ()
USING 'com.stratio.cassandra.lucene.Index'
WITH OPTIONS = {
   'refresh_seconds': '20',
   'schema': '{
      fields: {
         station_id: {type: "string"},
         year: {type: "integer"},
         measurement_time: {type: "timestamp"},
         place: {type: "geo_point", latitude: "lat", longitude: "lon"}
      }
   }'
};

CREATE CUSTOM INDEX campsite_calculations_index ON campsites.calculations ()
USING 'com.stratio.cassandra.lucene.Index'
WITH OPTIONS = {
   'refresh_seconds': '20',
   'schema': '{
      fields: {
         campsite_id: {type: "integer"},
         year: {type: "integer"},
         calculation_time: {type: "timestamp"},
         place: {type: "geo_point", latitude: "lat", longitude: "lon"}
      }
   }'
};