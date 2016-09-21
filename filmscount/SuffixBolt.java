package com.xinpaninjava.filmscount;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

/**
 * appends the suffix to the filmName
 */
@SuppressWarnings("serial")
public class SuffixBolt extends BaseBasicBolt {
	private FileWriter fw;

	/**
	 * prepare is the init method to specify the desPath to output the data
	 */
	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context) {
		// each call generate the new fileWriterso
		// we assign the fw as the class member
		try {
			fw = new FileWriter("/home/hadoop/stormoutput/" + UUID.randomUUID());
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * write to the local fileSystem by using fileWriter
	 */
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		String upperName = input.getString(0);
		String desName = upperName + System.currentTimeMillis();
		// write the filmsName to the desPath
		try {
			fw.write(desName);
			fw.write("\n");
			fw.flush();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

	}

	/**
	 * we don't need to send the data to the next receiver
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

}
