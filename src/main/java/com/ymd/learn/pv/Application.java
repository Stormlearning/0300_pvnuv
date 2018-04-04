package com.ymd.learn.pv;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;

public class Application {
	
    public static void main(String[] args) {
    	
    	TopologyBuilder builder = new TopologyBuilder();
    	
    	builder.setSpout("queuespout", new QueueSpout());
    	builder.setBolt("pvbolt", new PvBolt(), 4).shuffleGrouping("queuespout");
    	builder.setBolt("sumbolt", new SumBolt(), 1).globalGrouping("pvbolt");
    	
    	Config conf = new Config();
    	conf.setDebug(true);
    	
    	if(args.length > 0) {
    		try {
				StormSubmitter.submitTopology("pvtopo", conf, builder.createTopology());
			} catch (AlreadyAliveException | InvalidTopologyException | AuthorizationException e) {
				e.printStackTrace();
			}
    	} else {
    		LocalCluster cluster = new LocalCluster();
    		cluster.submitTopology("pvtopo", conf, builder.createTopology());
    	}
    	
    	
    	
    	
    }
}
