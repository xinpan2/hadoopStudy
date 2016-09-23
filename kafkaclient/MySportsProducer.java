package kafkaclient;

import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/**
 * MySportsProducer acts as the producer console in the command line tools,but
 * in this way more convenient.
 */
public class MySportsProducer {

	public static void main(String[] args) {
		// set the properties of kafka
		Properties originalProps = new Properties();
		originalProps.put("zk.connect", "xinpan5:2181,xinpan6:2181,xinpan7:2181");
		originalProps.put("metadata.broker.list", "xinpan5:9092,xinpan6:9092,xinpan7:9092");
		// the serialize class,cause we send the messages are the type of String
		originalProps.put("serializer.class", "kafka.serializer.StringEncoder");

		ProducerConfig config = new ProducerConfig(originalProps);
		// initialize the producer
		Producer<String, String> producer = new Producer<>(config);

		// send the message by specifying the topic
		for (int j = 1; j <= 20; j++) {
			// <String,String> means that <topic,message>
			producer.send(new KeyedMessage<String, String>("mysports", "my sport is " + j));
		}
	}

}
