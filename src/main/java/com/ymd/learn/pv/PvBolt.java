package com.ymd.learn.pv;

import java.util.Map;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class PvBolt implements IBasicBolt {

	private static final long serialVersionUID = -1724861823247709806L;

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("taskId", "count"));
	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	TopologyContext context;
	int count = 0;
	public void prepare(Map stormConf, TopologyContext context) {
		this.context = context;
	}

	public void execute(Tuple input, BasicOutputCollector collector) {
		String log = input.getStringByField("log");
		if(log != null) {
			count++;
			collector.emit(new Values(context.getThisTaskId(), count));
		}
	}

	public void cleanup() {
		
	}

}
