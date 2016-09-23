package stormandkafka.storm.combiner;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;
import stormandkafka.storm.bolt.SplitBolt;
import stormandkafka.storm.bolt.WriteBolt;
import stormandkafka.storm.spout.Spout;

/**
 * the Combiner is the class of connecting the storm and kafka.
 */
public class Combiner {
	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
		// set the broker's host
		String zkRoot = "/kafka-storm";
		String spoutId = "KafkaSpout";
		BrokerHosts brokerHosts = new ZkHosts("xinpan5:2181,xinpan6:2181,xinpan7:2181");
		SpoutConfig spoutConfig = new SpoutConfig(brokerHosts, "wordcount", zkRoot, spoutId);
		spoutConfig.forceFromStart = true;// from beginning to read the message
		// assign the spout
		spoutConfig.scheme = new SchemeAsMultiScheme(new Spout());
		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("KafkaSpout", new KafkaSpout(spoutConfig));
		builder.setBolt("word-spilter", new SplitBolt()).shuffleGrouping(spoutId);
		// we need to set all the content to 4 files by different fields
		builder.setBolt("writer", new WriteBolt(), 4).fieldsGrouping("word-spilter", new Fields("word"));

		Config config = new Config();
		config.setDebug(true);
		config.setNumAckers(0);
		config.setNumWorkers(4);

		// submit the topology
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("WordCount", config, builder.createTopology());
		// StormSubmitter.submitTopology(topic, config,
		// topology.createTopology());
	}
}
