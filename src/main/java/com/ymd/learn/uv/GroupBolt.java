package com.ymd.learn.uv;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class GroupBolt implements IBasicBolt {

	private static final long serialVersionUID = -8522290537913677408L;

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("date", "sessionidset"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		
	}
	
	//Map<date, set<session>>
	Map<String, Set<String>> sessionMap = new HashMap<String, Set<String>>();
	
	
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		String sessionId = input.getStringByField("sessionid");
		String date = input.getStringByField("date");
		
		if(!sessionMap.containsKey(date)) {
			sessionMap.put(date, new HashSet<String>());
		}
		
		sessionMap.get(date).add(sessionId);
		collector.emit(new Values(date, sessionMap.get(date)));
	}

	@Override
	public void cleanup() {
		
	}

}
