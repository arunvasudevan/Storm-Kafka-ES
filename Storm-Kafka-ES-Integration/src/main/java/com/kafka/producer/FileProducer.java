package com.kafka.producer;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class FileProducer {
	Properties props=new Properties();
	static ProducerConfig config;
	
	public FileProducer(){
	
	props.put("metadata.broker.list","localhost:9092");
	props.put("serializer.class","kafka.serializer.StringEncoder");
	props.put("request.required.acks","1");
	
	config=new ProducerConfig(props);
	}
	
	
	public static void main(String args[]) throws IOException{
		new FileProducer();
		
		Producer<String,String> producer=new Producer<String,String>(config);
		String str=null;
		
		BufferedReader bf=new BufferedReader(new FileReader(new File("/Users/Arun/Documents/StockData/BAC.txt")));
		
		while((str=bf.readLine())!=null){
			KeyedMessage<String, String> k=new KeyedMessage<String, String>("StockData", str);
			System.out.println(str);
			producer.send(k);
		}
		bf.close();
		producer.close();
	}
	
}
