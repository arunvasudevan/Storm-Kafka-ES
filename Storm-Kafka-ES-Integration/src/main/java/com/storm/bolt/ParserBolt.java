package com.storm.bolt;

import java.util.Map;

import net.sf.json.JSONObject;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class ParserBolt extends BaseRichBolt {

	/**
	 * Arun Vasudevan
	 */
	private static final long serialVersionUID = 1L;
	OutputCollector _collector;
	// Declare the Metadata of the incoming file
	String[] columnArray = { "source","stream","id","Date", "Open", "High", "Low", "Close", "Volume",
			"Adj Close" };

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map arg0, TopologyContext arg1,
			OutputCollector collector) {
		_collector = collector;
	}

	@Override
	public void execute(Tuple arg0) {
		String[] stockData = arg0.toString().replace("[", "").replace("]", "").split(",");
		JSONObject json = new JSONObject();
		int i = 0;
		
		for (String line : stockData) {
			System.out.println(i+":"+columnArray[i]+":"+line);
			
			json.put(columnArray[i++], line.replace(":", "").trim());
			
		}

		_collector.emit(new Values(json));
		_collector.ack(arg0);

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("Close"));

	}

}
