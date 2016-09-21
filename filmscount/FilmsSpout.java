package com.xinpaninjava.filmscount;

import java.util.Map;
import java.util.Random;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

/**
 * FilmsSpout as the component of spout. We utilize this class to simulate the
 * spout handling the quantities data flows.
 */
@SuppressWarnings("serial")
public class FilmsSpout extends BaseRichSpout {
	// This output collector exposes the API for emitting tuples from an
	// IRichSpout.
	private SpoutOutputCollector collector;

	// to use the array as the data source
	String[] filmsName = { "All Quiet on the Western Front", "Street Angel", "Gone with the Wind", "Waterloo Bridge",
			"Citizen Kane", "Casablanca", "Spring in a Small Town" };

	/**
	 * Called when a task for this component is initialized within a worker on
	 * the cluster. It provides the spout with the environment in which the
	 * spout executes.
	 */
	@Override
	public void open(@SuppressWarnings("rawtypes") Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
	}

	/**
	 * aims to point out the next receiver
	 */
	@Override
	public void nextTuple() {
		// simulate the steady streams of data
		Random random = new Random();
		int index = random.nextInt(filmsName.length);
		collector.emit(new Values(filmsName[index]));

		Utils.sleep(500);// stop for a while to clearly analyze the result
	}

	/**
	 * to declare the field name of the date that is emitted to the next
	 * receiver
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("originName"));
	}

}
