package com.ymd.learn.uv;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

public class SumBolt implements IRichBolt {

	private static final long serialVersionUID = 380022242088351468L;

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		
	}
	
	Map<String, Set<String>> sessionMap = new HashMap<String, Set<String>>();

	@SuppressWarnings("unchecked")
	@Override
	public void execute(Tuple input) {
		String date = input.getStringByField("date");
		Set<String> sessionIdSet = (Set<String>)input.getValue(1);
		
		sessionMap.put(date, sessionIdSet);
		
		System.err.println("date -- "+ date + ";-----sessionIds" + sessionIdSet.size() + "seesions = " + sessionIdSet);
	}

	@Override
	public void cleanup() {
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
