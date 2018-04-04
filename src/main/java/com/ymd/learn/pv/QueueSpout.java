package com.ymd.learn.pv;

import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class QueueSpout implements IRichSpout {

	private static final long serialVersionUID = 7821569713865616515L;
	
	LinkedBlockingQueue<String> sourcQueue;
	SpoutOutputCollector collector;
	
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		sourcQueue = new LinkedBlockingQueue<String>();
		
		String[] hosts = {"www.taobao.com"};
		String[] sessionIds = new String[5];
		sessionIds[0] = genSessionId();
		sessionIds[1] = genSessionId();
		sessionIds[2] = genSessionId();
		sessionIds[3] = genSessionId();
		sessionIds[4] = genSessionId();
		String[] times = {"2017-07-01 09:00:10", "2017-07-01 10:20:00", "2017-07-01 12:30:00", 
				"2017-07-01 14:00:02", "2017-07-01 18:00:09"};
		Random random = new Random();
		for(int i=0; i<50; i++) {
			sourcQueue.add(hosts[0] + "&&" + sessionIds[random.nextInt(sessionIds.length)] + "&&" +
					times[random.nextInt(times.length)]);
		}
		
		this.collector = collector;
	}
	
	private static String genSessionId() {
		return UUID.randomUUID().toString();
	}
	
	public void close() {
		
	}

	public void activate() {
		
	}

	public void deactivate() {
		
	}

	public void nextTuple() {
		try {
			this.collector.emit(new Values(this.sourcQueue.take()));
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public void ack(Object msgId) {
		
	}

	public void fail(Object msgId) {
		
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("log"));
	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}
