package kafkaclient;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

/**
 * MySportsConsumer acts as the consumer console in the command line tools.
 */
public class MySportsConsumer {
	public static void main(String[] args) {
		/*
		 * auto.offset.reset: the strategy of reading the message from where we
		 * set,the beginning or others offset
		 */
		Properties originalProps = new Properties();
		originalProps.put("zookeeper.connect", "xinpan5:2181,xinpan6:2181,xinpan7:2181");
		originalProps.put("group.id", "1111");
		originalProps.put("auto.offset.reset", "smallest");

		ConsumerConfig config = new ConsumerConfig(originalProps);
		// initialize the consumer
		ConsumerConnector consumer = Consumer.createJavaConsumerConnector(config);

		/*
		 * set the number of thread to run the topic,<topic,number>. In this
		 * map,we can assign more than one topics
		 */
		Map<String, Integer> topicCountMap = new HashMap<>();
		topicCountMap.put("mysports", 1);

		// create the stream of reading messages
		Map<String, List<KafkaStream<byte[], byte[]>>> streams = consumer.createMessageStreams(topicCountMap);

		
		/*
		 * in the streams will have the number of topicCountMap.size() of the
		 * topic's message,we get the specificity topic by topic name
		 */
		List<KafkaStream<byte[], byte[]>> mysports = streams.get("mysports");

		for (KafkaStream<byte[], byte[]> messageAndMetadatas : mysports) {
			//traditional method to run a thread
			
//			new Thread(new Runnable() {
//				@Override
//				public void run() {
//					for(MessageAndMetadata<byte[], byte[]> messageAndMetadata : messageAndMetadatas){
//						String message = new String(messageAndMetadata.message());
//						System.out.println(message);
//					}
//				}
//			
//			}).start();
			
			//lambda expression
			new Thread(() -> {
				for (MessageAndMetadata<byte[], byte[]> messageAndMetadata : messageAndMetadatas) {
					String message = new String(messageAndMetadata.message());
					System.out.println(message);
				}
			}).start();
		}
	}
}
