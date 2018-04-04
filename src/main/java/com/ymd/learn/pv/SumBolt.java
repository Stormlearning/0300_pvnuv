package com.ymd.learn.pv;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

public class SumBolt implements IRichBolt {

	private static final long serialVersionUID = 4983443588601460240L;

	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		
	}
	
	Map<Integer, Integer> pvMap = new HashMap<Integer, Integer>();
	
	
	public void execute(Tuple input) {
		Integer taskId = input.getIntegerByField("taskId");
		Integer count = input.getIntegerByField("count");
		
		pvMap.put(taskId, count);
		
		int totalCount = pvMap.values().stream().mapToInt(i -> i).sum();
		
		System.out.println("total num = " + totalCount);
		
	}

	public void cleanup() {
		
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
