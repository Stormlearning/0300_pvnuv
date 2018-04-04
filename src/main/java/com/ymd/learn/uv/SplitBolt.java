package com.ymd.learn.uv;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

/**
 * 
 * log -> sessionId, datetime
 *
 */
public class SplitBolt implements IRichBolt {

	private static final long serialVersionUID = -8740299765969115785L;
	
	OutputCollector collector; 
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		String log = input.getStringByField("log");
		if(log != null) {
			String[] dataArr = log.split("&&");
			if(dataArr.length == 3) {
				collector.emit(new Values(dataArr[1], DateUtils.toDate(dataArr[2])));
			}
		}
	}

	@Override
	public void cleanup() {
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("sessionid", "date"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
