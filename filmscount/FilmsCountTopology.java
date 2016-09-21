package com.xinpaninjava.filmscount;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;

/**
 * connect all the components as the topology to work together
 */
public class FilmsCountTopology {
	public static void main(String[] args) throws Exception {
		// initialize the topologyBuilder
		TopologyBuilder topologyBuilder = new TopologyBuilder();
		/*
		 * connect all the components,(String id, IRichSpout spout, Number parallelism_hint)
		 * 
		 * parallelism_hint: the executor number of this component
		 * 
		 * setNumTasks(8): each executor run 8/4=2 tasks
		 * 
		 * shuffleGrouping: specifies of receiving the data from where and
		 * 					chooses which strategy to distribute the data
		 */
		topologyBuilder.setSpout("filmsSpout", new FilmsSpout(), 4).setNumTasks(8);
		topologyBuilder.setBolt("upperBole", new UpperBolt(), 4).shuffleGrouping("filmsSpout");
		topologyBuilder.setBolt("SuffixBolt", new SuffixBolt(), 4).shuffleGrouping("upperBole");
		
		// after connect all the components,create the topology
		StormTopology filmsCount = topologyBuilder.createTopology();
		
		// set corresponding configurations
		Config config = new Config();
		config.setNumWorkers(4);
		config.setDebug(true);// generate logs
		// validates the result of the topology,if the different value match to 0,throws exception
		config.setNumAckers(0);

		// submit the topology
		StormSubmitter.submitTopology("fimesCount", config, filmsCount);
	}
}
