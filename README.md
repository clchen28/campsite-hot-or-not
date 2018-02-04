# Campsite Hot or Not
A data pipeline to show calculated historical weather at U.S. campsites based on the nearest weather stations, using the 260 GB NOAA weather dataset, built during my time as a Data Engineering Fellow at Insight Data Science.

## Purpose / Use Cases
The purpose is to build a data pipeline that will support a web application that will show both historic and realtime weather conditions at campsites. The NOAA weather dataset is a dataset that contains a weather summary for every day dating back several decades (260 GB). It contains weather conditions at every individual weather station, including temperature, wind conditions, and so on. There is also a list of all campsites in the U.S. (~4000).

The end product is a web application that allows a user to find the historical weather at a campsite for a specific day by choosing a campsite on a map.

The core project was to build out a data pipeline that would perform the data transformations needed to get the above functionality, store the data to a database, and to serve the web app.

Campsites are at different locations than weather stations, so in order to calculate the weather at a campsite, a weighted average from nearby weather stations needs to be calculated. The weight is a function of distance - closer weather stations should have a stronger effect on the calculation of the weather.

## Architecture

![Architecture](https://raw.githubusercontent.com/CCInCharge/campsite-hot-or-not/master/img/pipeline.png "Architecture")

## File Storage
**S3**. HDFS is more performant than S3. There will be a lot of compute operations on the data, as the batch process is iterated on. However, S3 is a lot cheaper than HDFS, and since it is a managed service, getting the file store up and running will be a lot quicker. These are important considerations for a project that will be utilizing Insight's resources, as well as a project that requires a very quick turnaround time. I chose to go with S3.

## Batch Computation Framework
**Spark**. Both Spark and Flink have batch processing frameworks, but are built with different mindsets. Spark is primarily a batch-oriented framework, and views streaming as a batch process over a small period of time (e.g. 1 s). Flink is focused on truly real-time processing, and views batch processing as a special case of stream processing (bounded stream processing). Flink has lower latency than Spark, but for this application, achieving a small decrease in latency for the computations will not affect our results much, and the results are not mission critical. Additionally, using one common tool helps to reduce the complexity of the codebase - the batch and streaming portions of the pipeline can reuse much of the same code.

## Database
**Cassandra**. The raw data from NOAA dates back two decades, and the end goal is to store hourly data, for both weather stations, and for campsites. This means that the data is time-series in nature. Cassandra's column-oriented architecture is a good fit for time-series data.

## Engineering Challenge
The selected implementation of the IDW Average algorithm presents an engineering challenge. This algorithm is two steps - finding the k nearest nodes from a campsite, and then calculating the weighted average. The brute force method of doing this is to, for each campsite and historical time, calculate the distance to all weather stations that have data available at that time, and then choose the 5 that have the shortest distance. There are also ways to optimize this algorithm.

Another way to solve this is to use some type of data structure that will efficiently find the k nearest neighbors, such as a KD-tree. This would be a quite complex way to solve this problem very efficiently.
