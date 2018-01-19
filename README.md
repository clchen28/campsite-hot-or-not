# Campsite Weather
A data pipeline to show calculated realtime and historical weather at U.S. campsites based on the nearest weather stations, using the NOAA weather dataset.

## Purpose / Use Cases
The purpose is to build a data pipeline that will support a web application that will show both historic and realtime weather conditions at campsites. The NOAA weather dataset is a dataset that contains a weather summary for every day dating back several decades (> 100 GB). It contains weather conditions at every individual weather station, including temperature, wind conditions, and so on. There is also a list of all campsites in the U.S. (~4000).

The end product is a web application that allows a user to find the historical weather at a campsite for a specific day by choosing a campsite on a map, as well as to allow a user to see the average weather over a range of dates or for a specific date (e.g., February 3 for all years) on a campsite. Additionally, the user should be able to see realtime weather conditions for all campsites in a region. Users should also be able to click on an arbitrary point in the map and see the weather at that location - this functionality should support the possibility that many users are issuing requests for weather in a relatively small region.

The core project will focus on getting the batch and streaming pipelines set up to support the queries mentioned above. If there is time, there can be a few more things done to increase the complexity of the problem, as well as to stress test the system and determine benchmarks.

Campsites are at different locations than weather stations, so in order to calculate the weather at a campsite, the nearest k weather stations should be used. Some sort of weighted average should be used. The weather station that is closest to a campsite should have the most effect on the calculated weather at the campsite when compared to other campsites, so some algorithm such as an Inverted Distance Weight should be used when computing the weather to take this into account. This needs to take into the account the fact that, for a given campsite, the nearest k weather stations will not be the same for all days - certain weather stations will be down on some days, and when going back further historically, there will be fewer active weather stations.

For realtime data, NOAA has an API that can pull the latest weather. Unfortunately, this is limited to 1000 requests per day, which correlates to about 1 request per 90 seconds. One possibility is to use the real data and then generate fake weather readings for every weather station, twice a second, to increase the stress on the realtime data pipeline. The realtime system should calculate the weather using the nearest valid weather stations at each campsite, but should also support the possibility of users querying the weather for a random location. If there is time to implement this feature, the pipeline should also be able to accommodate the possibility that many users are querying weather information for a small region. This may require some novel solution for intelligently partitioning the analytics database (e.g., if data is partitioned to different nodes via equal-area geographical regions, data skew will be introduced for certain regions), as well as thoughtful data caching.

The applications of this project extend beyond just finding weather at campsites. Using data collected at discrete points in space to estimate a continuous distribution of data at any point in space is a challenge that is applicable to many domains - calculating local pressure at a desired point using an array of pressure sensors, estimating traffic flow in a stretch of road using location data from multiple phones, and estimating WiFi strength in a certain location given the location of WiFi hotspots are just some of the many possibilities. Additionally, intelligently handling data skew is a challenge that is important for all big data pipelines

### Queries
#### Historical (Batch)
- Weather at a given campsite for a given date
- Weather average at a given campsite for a range of dates
- Weather average at a given campsite for a specific date over many years (Feburary 3rd for all years)

#### Current (Streaming)
- Best weather conditions for a campsite within a certain radius from a search point
- Current weather conditions at a campsite
- Current weather conditions at an arbitrary location

## Technologies
File Storage Possibilities:
- S3
- HDFS

File Storage Choice: **S3**. HDFS is more performant than S3. There will be a lot of compute operations on the data, as the batch process is iterated on. However, S3 is a lot cheaper than HDFS, and since it is a manged service, getting the file store up and running will be a lot quicker. These are important considerations for a project that will be utilizing Insight's resources, as well as a project that requires a very quick turnaround time.

### Ingestion:
- RabbitMQ
- Kafka
- Amazon Kinesis

Ingestion Choice: **Kafka**. For any application that is related to IoT, it can be expected that there will be a very large number of events that are being emitted per second. Additionally, there isn't very complicated routing logic for the data between producers and consumers. Kafka has higher throughput and lower latency than Kinesis and RabbitMQ. RabbitMQ is useful when complicated data routing is required, and when stronger guarantees for consistency and data arriving in a well-defined order are required.

### Computations:
#### Batch
- Spark
- Flink
- Spark GraphX
- Hive
- Pig
- MapReduce
- Giraph

#### Streaming
- Kafka Streams
- Spark Streaming
- Flink
- Storm

Computation choice: **Spark**. Both Spark and Flink are unified processing frameworks, but are built with different mindsets. Spark is primarily a batch-oriented framework, and views streaming as a batch process over a small period of time (e.g. 1 s). Flink is focused on truly real-time processing, and views batch processing as a special case of stream processing (bounded stream processing). Flink has lower latency than Spark, but for this application, achieving a minute decrease in latency for the computations will not affect our results much, and the results are not mission critical. Additionally, using one common tool helps to reduce the complexity of the codebase - the batch and streaming portions of the pipeline can reuse much of the same code.

#### Databases
##### Analytics Database
- Neo4j
- Postgres
- ElasticSearch
- Cassandra

Analytics database choice: **Cassandra**. One challenge is going to be to find the nearest k weather stations when executing a query. Some graph-based databases will be very efficient for this (Neo4j). Additionally, there are some databases that have built in GIS capabilities (Postgres, ElasticSearch) that can help calculate the distance between various locations. Postgres is designed to scale vertically, not horizontally - while scaling horizontally is possible, it's difficult with Postgres. However, column oriented databases like Cassandra make range queries very efficient. Finding the nearest k neighbors can be done by doing a range query for the windstations based on laitude/longitude, which Cassandra supports well. Additionally, the user will want to run range queries over certain dates, and Cassandra is better suited for this task than a graph database.

##### Cache Database
- Redis
- Memcached

Cache choice: **Redis**. Memcached is great for caching small datasets or static datasets. Redis can store data structures that map to a key. This can be used to store the latest data that has arrived from a given weather station, and subsequent requests for data can simply scan these data structures for the nearest nodes as opposed to querying the database.

## Engineering Challenge
The selected implementation of the IDW Average algorithm presents an engineering challenge. This algorithm is two steps - finding the k nearest nodes from a campsite, and then calculating the weighted average. The brute force method of doing this is to, for each campsite and historical time, calculate the distance to all weather stations that have data available at that time, and then choose the 5 that have the shortest distance. There are also ways to optimize this algorithm.

Another way to solve this is to use some type of data structure that will efficiently find the k nearest neighbors, such as a KD-tree. This would be a quite complex way to solve this problem very efficiently.

## Architecture
S3 -> Kafka -> Spark -> Cassandra for batch.

API/Live Data/User Requests -> Kafka -> Spark -> Cassandra for real-time.

The last available weather station data can be cached in Redis.