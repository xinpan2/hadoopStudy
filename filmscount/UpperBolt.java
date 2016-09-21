package com.xinpaninjava.filmscount;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * the UpperBolt transfers the filmsNmae to the UpperCase
 */
@SuppressWarnings("serial")
public class UpperBolt extends BaseBasicBolt {
	/**
	 * the main logical process of the bolt to change the case to the upper
	 */
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		// get the data from the spout,due to the tuple we send to
		// this bolt only has one field ,we get the index 0 of the tuple
		String filmsName = input.getString(0);
		// turn it to the upper
		String upperName = filmsName.toUpperCase();
		// send to the next receiver
		collector.emit(new Values(upperName));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// declare the name of the field we send to the next bolt
		declarer.declare(new Fields("upperName"));
	}

}
