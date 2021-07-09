# SABD_Project2
## Project description

Below is given a brief description of the folders that make up the project.

### docker

This folder contains scripts and yml file to manage containers. A more accurate description
can be found in a specific README present in the docker folder.

### data

Directory containing the given .csv dataset.

### Results
In this folder can be found all the outputs relative to the required queries. In particular:
* *query1_monthly.csv*, contains query1 monthly results;
* *query1_weekly.csv*, contains query1 weekly results;
* *query2_monthly.csv*, contains query2 monthly results;
* *query2_weekly.csv*, contains query2 weekly results;
* *query3_1hour.csv*, contains query3 results for 1 hour;
* *query3_2hour.csv*, contains query3 results for 2 hours;

### src/main/java
Here is contained the main Java code for the project. In particular, it is divided into four packages:

* **config** representing the properties and addresses configuration for the project. 
It is composed by the **Configuration.java** class.
  

* **flink** representing the code for the data processing in Flink. It is further divided in the packages 
  **query1**, **query2** and **query3**, described next.
  

* **query1** containing the code for the query 1 implementation. It's divided in:
    * *Query1Topology.java*, contains the query1 topology implementation;
    * *AverageShipsAggregator.java*, contains the query1 aggregator implementation, 
    *  responsible to aggregate ships sum by type;
    *  *AverageShipsAccumulator.java*, implements the adding logic for the aggregator;
    *  *AverageProcessWindow.java*, is responsible to assign the window right start date and
  to compute the daily average based on the window size;
    *  *AverageOutcome.java*, contains the outcome object for every window shift;
    * *Query1Result.java*, represents the result object to extract the final output. 
* **query2** containing the code for the query 2 implementation. It's divided in:
  * *Query2Topology.java*, contains the query2 topology implementation;
  * *RankingCellAggregator.java*, contains the query2 aggregator implementation,
  *  responsible to rank cells by frequency of ships crossing, dividing by AM and PM;
  *  *RankingCellAccumulator.java*, implements the adding logic for the aggregator, 
     and the division between AM and PM;
  *  *RankingProcessWindow.java*, is responsible to assign the window right start date;
  *  *RankingOutcome.java*, contains the outcome object for every window shift.
* **query3** containing the code for the query 3 implementation. It's divided in:
  * *Query3Topology.java*, contains the query3 topology implementation;
  * *DistanceAggregator.java*, contains the query3 aggregator implementation,
  *  responsible to rank the first 5 tripIDs that have traveled the longest distance;
  *  *DistanceAccumulator.java*, implements the adding logic for the aggregator,
     by calculating the last travelled distance for each trip;
  *  *DistanceProcessWindow.java*, is responsible to assign the window right start date;
  *  *DistanceOutcome.java*, contains the outcome object for every window shift;
  *  *DistanceCounter.java*, contains the object for the distance computation. 
  


* **kafka** containing Producer and Consumer implementation for Kafka. It's, therefore, 
  divided in:
  * *SimpleKafkaConsumer.java*, contains the consumer implementation;
  * *SimpleKafkaProducer.java*, contains the producer implementation;
  

* **utils** containing utilities for the project, further divided in the packages **beans**,
  **kafka_utils**, **metrics** and **queries_utils**, described next.
  

*  **beans** containing the objects utilized by all the queries. It's divided in:
    * *ShipData.java*, contains the trips information required by queries.
      
    * *SpeedUpEventTim.java*, contains the event setters and getters fot the dataset replay.  
*  **kafka_utils** containing the utilities utilized by Kafka. It's composed by:
   * *FlinkStringToKafkaSerializer.java*, contains the results' serialization to allow data to be
     correctly published to Kafka.
     
*  **metrics** containing the throughput and latency computation. It's divided in:
    * *Metrics.java*, implements the incremental counter to update metrics;
    * *MetricsInvoker.java*, contains the MetricInvoker to instantiate as a Sink to obtain
    the desired results.
   
*  **queries_utils** containing the utilities utilized by Flink. It's divided in:
   * *ComputeCell.java*, implements the logic to extract the right sector from latitude and
     longitude information;
   * *ResultsUtils.java*, contains the Results paths, and the windows offsets computation;
   * *dates.json*, is a Json containing the datasets first dates, divided respect to the
     Occidental and Oriental Sea, necessary to the windows offsets' computation.

Finally, the Main Classes to start the application are:
* *ConsumerLauncher.java*, class used to launch consumers for Flink output;
* *ProducerLauncher.java*, class used to start a producer that reads from file and sends tuples to Kafka topics;
* *FlinkMain.java*, contains the Flink environment set up and calls the queries by passing them the
filtered stream source.