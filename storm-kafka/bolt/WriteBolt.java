package stormandkafka.storm.bolt;

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
 * this bolt writes the word to the local fileSystem
 */
public class WriteBolt extends BaseBasicBolt {

	private static final long serialVersionUID = -7860048781639213915L;

	private FileWriter fw;

	/**
	 * cause each call to this bolt need to write to the fileSystem,we
	 * initialize the fileWrite in this initial method
	 */
	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf, TopologyContext context) {
		try {
			fw = new FileWriter("c:\\wordCount" + UUID.randomUUID().toString());
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * get the message from the SplitBolt and we just need to write all the word
	 * to the local fileSystem
	 */
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		String word = input.getString(0);
		try {// write to the fileSystem
			fw.write(word);
			fw.write("\n");
			fw.flush();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	// we don't need to send the tuple to the next bolt
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

}
