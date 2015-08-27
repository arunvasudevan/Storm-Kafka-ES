package com.storm.topology;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.elasticsearch.storm.EsBolt;

import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;

import com.storm.bolt.ParserBolt;

/**
 * @author Arun Vasudevan
 * This Storm Topology reads messages from Kafka using a Kafka Spout 
 * Parses the messages read using a Bolt into a JSON
 * Inserts into Elastic search 
 *
 */
public class StockAnalysisTopology {
	
	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException{
		TopologyBuilder builder=new TopologyBuilder();
		
		ZkHosts zkhost=new ZkHosts("localhost:2181");
		// Spout Configuration with Zoo-keeper Host, Topic Name, Zoo-keeper root and offset
		SpoutConfig spoutConf=new SpoutConfig(zkhost, "StockData", "/StockData", UUID.randomUUID().toString());
		// As Default scheme for Kafka is Byte Stream we should change it to String Scheme
		spoutConf.scheme=new SchemeAsMultiScheme(new StringScheme());
		
		// Set the Kafka spout to the topology builder
		builder.setSpout("KafkaSpout", new KafkaSpout(spoutConf));
		// Set the Parser Bolt to the Topology
		builder.setBolt("ParserBolt", new ParserBolt()).shuffleGrouping("KafkaSpout");
		
		// Elastic search Configuration to set the input type as JSON
		Map<Object, Object> esConf=new HashMap<Object, Object>();
		esConf.put("es.input.json", "true");
		
		// Set the Elastic search index as stockdata and type as docs...
		builder.setBolt("ESbolt", new EsBolt("stockdata/docs", esConf)).shuffleGrouping("ParserBolt");
		
		Config stormConf=new Config();
		
		try{
			// Submit the topology using the Storm Submitter
			StormSubmitter.submitTopology("StockDataTopology", stormConf, builder.createTopology());
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	
	

}
