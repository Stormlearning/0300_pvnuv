package com.ymd.learn.uv;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class Application {
	
	public static void main(String[] args) {
		
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("queuespout", new QueueSpout());
		builder.setBolt("splitbolt", new SplitBolt(), 4).shuffleGrouping("queuespout");
		builder.setBolt("groupbolt", new GroupBolt(), 4).fieldsGrouping("splitbolt", new Fields("date"));
		builder.setBolt("sumbolt", new SumBolt(), 1).shuffleGrouping("groupbolt");
		
		Config config = new Config();
		config.setDebug(false);
		
		if(args.length > 0) {
			try {
				StormSubmitter.submitTopology("uvtopo", config, builder.createTopology());
			} catch (AlreadyAliveException | InvalidTopologyException | AuthorizationException e) {
				e.printStackTrace();
			}
		} else {
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("uvtopo", config, builder.createTopology());
		}
	}
	
	
}
