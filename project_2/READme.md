# Project Description

This project involves building a real-time data processing pipeline using Apache Spark, Kafka, and Cassandra. The objective is to create a Structured Streaming Spark process that consumes Kafka messages, processes them, and persists the information in a Cassandra NoSQL database. The project is divided into two parts:

## Part I: Kafka Stream Generation
- Create a Python script to simulate a stream of songs listened to by a group of people using the Faker library and a provided `spotify-songs.csv` file.
- The script generates periodic messages (once per minute), including the listener's name, the song title, and the timestamp, and sends them to a Kafka topic.
- Your own name must be included in the list of listeners.

## Part II: Spark Streaming and Cassandra Persistence
- Develop a PySpark script to consume the Kafka messages, process them, and persist the data in Cassandra.
- For each message, store the listener's name, timestamp, and song details in Cassandra.
- Optimize the Cassandra schema to support queries such as calculating the average danceability of songs a person listened to during a specific hour.
- Use Spark caching to improve performance and configure a persistence interval (default 30 seconds).

## Execution and Reporting
- The project includes Vagrant, Docker, and various scripts to facilitate execution.
- You are required to submit:
  1. Python and PySpark scripts.
  2. Cassandra data model details.
  3. Sample persisted data (around 50 rows).
  4. Two CQL queries retrieving song names and average danceability for a specific person and hour.
