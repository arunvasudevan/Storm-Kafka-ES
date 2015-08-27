# Storm-Kafka-ES
Storm Topology to Integrate storm with Kafka and Elasticsearch

This Storm Topology reads messages from Kafka using a Kafka Spout and Parses the incoming messages read from Kafka using a Bolt into a JSON message.
Parsed JSON message is then loaded on to Elastic search for dashboarding and analysis using Kibana

Prerequisite for this Project:
Zookeeper Installation
Kafka Broker
Storm Installation and start Nimbus, Supervisor, Logviewer and UI
Elastic search
Kibana (for Dashboarding)

Setup:
After installing the above tool sets download the maven project into the local workspace and build a jar 

Execution:
storm jar <Workspace Path>/Storm-Kafka-ES-Integration/target/StormKafkaESIntegration-0.0.1-SNAPSHOT-jar-with-dependencies.jar com.storm.topology.StockAnalysisTopology
