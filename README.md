# Heathrow Flights Streaming and ETL
This is a sample code for Apache Beam to perform ETL from a stream-processing service (Pub/Sub) to BigQuery using Dataflow as the runner. 

## Overview
An implementation of Apache Beam to stream the active flights either going to or leaving London Heathrow airport using Pub/Sub, process the data by joining with a dimensional table, and load them to a BigQuery table. 

The project consists of the following Java classes for the ETL pipeline:

1. com.streaming.ETL - Main method for injesting the Pub/Sub messages and the dimensional table, and loading the processed data to BigQuery
2. com.streaming.messageParsing - Class for parsing the injested Pub/Sub messages into BigQuery row objects (TableRow)
3. com.streaming.aggregate - Class for converting TableRows to Key/Value pairs that can be grouped by a key and aggregated
4. com.streaming.join - Class for joining the aggregated Pub/Sub messages with TableRows from the dimensional table 

The project also consites of the following scripts:

1. pom.xml - Configuration file for Apache Maven
2. message_subscriber - Directory with the com.subscriber.Subscriber Java Class and pom.xml file for Google Cloud Functions to receive and publish the injested messages containing the flight data

## Executing Apache Beam ETL Pipelines on Google Dataflow using the Cloud SDK

From the same directory as the Apache Beam project execute:

    ```shell
    mvn compile exec:java \
    -Dexec.mainClass=com.streaming.ETL \
    -Dexec.cleanupDaemonThreads=false \
    -Dexec.args=" \
        --project=${PROJECT} \
        --region=${REGION} \
        --inputTopic=${TOPIC} \
        --runner=DataflowRunner \
        --windowSize=${WINDOW}"
    ```

## References

This project relies on the following resources:

1. streaming data source [Heathrow Flights](https://ably.com/hub/ably-flightradar24/heathrow-flights) from [Ably Hub](https://ably.com/hub)
2. [OpenFlights](https://github.com/jpatokal/openflights) dataset for the dimensional tables with airline and aircraft data