package stormandkafka.storm.bolt;

import org.apache.commons.lang.StringUtils;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * the bolt implements like we did in the single storm framework.
 */
public class SplitBolt extends BaseBasicBolt {

	private static final long serialVersionUID = -4570562881044214377L;

	/**
	 * get the message from the spout and we just need to split all the
	 * sentences to a single word
	 */
	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		String line = input.getString(0);// get tuple
		String[] words = line.split(" ");// split by blank

		for (String word : words) {
			word = word.trim();
			if (StringUtils.isNotBlank(word)) {
				collector.emit(new Values(word));
			}
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}

}
