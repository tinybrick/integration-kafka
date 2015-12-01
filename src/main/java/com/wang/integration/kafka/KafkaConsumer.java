package com.wang.integration.kafka;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.ConsumerTimeoutException;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

import org.apache.log4j.Logger;

import com.wang.integration.kafka.event.KafkaMessageHandler;
import com.wang.integration.message.IMessageConsumer;

public class KafkaConsumer extends KafkaMessageHandler implements IMessageConsumer {
	Logger logger = Logger.getLogger(this.getClass());

	public List<ExecutorService> executorManager = new ArrayList<ExecutorService>();

	private ConsumerConnector consumer;
	public int numThreads = 0;

	public int getNumThreads() {
		return numThreads;
	}

	public void setNumThreads(int numThreads) {
		this.numThreads = numThreads;
	}

	public KafkaConsumer(Properties props) {
		this(props, false);
	}

	public KafkaConsumer(Properties props, String group) {
		props.put("group.id", "Group_" + group);

		ConsumerConfig config = new ConsumerConfig(props);
		consumer = Consumer.createJavaConsumerConnector(config);
	}

	public KafkaConsumer(Properties props, boolean randomGroup) {
		if (randomGroup) {
			Random random = new Random();
			props.put("group.id", "Group_" + random.nextInt(100));
		}

		ConsumerConfig config = new ConsumerConfig(props);
		consumer = Consumer.createJavaConsumerConnector(config);
	}

	/* (non-Javadoc)
	 * @see com.wang.integration.kafka.IMessageConsumer#subscribe(java.lang.String)
	 */
	@Override
	public void subscribe(String topic) {
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();

		if (0 == numThreads) {
			throw new RuntimeException("Partition number must not be 0");
		}

		// create threads for consume
		topicCountMap.put(topic, new Integer(numThreads));
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);

		Set<String> topics = consumerMap.keySet();
		for (String topicName : topics) {
			List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topicName);

			// create 3 threads to consume from each of the partitions
			ExecutorService executor = Executors.newFixedThreadPool(streams.size());
			executorManager.add(executor);

			try {
				for (final KafkaStream<byte[], byte[]> stream : streams) {
					final ConsumerIterator<byte[], byte[]> consumerIterator = stream.iterator();

					executor.submit(new Thread() {
						public void run() {
							boolean timeout = true;

							while (timeout) {
								try {
									while (consumerIterator.hasNext()) {
										timeout = false;
										/**
										 * hasNext() will NOT change offset, but next() DO. Call
										 * consumer.commitOffsets() to commit offset.
										 */
										MessageAndMetadata<byte[], byte[]> messageAndMetadata = consumerIterator.next();
										KafkaMessageEvent messageEvent = new KafkaMessageEvent(messageAndMetadata);
										processEvent(messageEvent, getId());
									}
								}
								catch (ConsumerTimeoutException e) {
									logger.error(e.getMessage(), e);
									timeout = true;
								}
								catch (Exception e) {
									logger.error(e.getMessage(), e);
									shutdown();
									throw e;
								}
							}
						}
					});
				}
			}
			catch (Exception e) {
				logger.error(e.getMessage(), e);
			}
		}
	}

	public void shutdown() {
		consumer.commitOffsets();
		consumer.shutdown();
	}

	public void processEvent(KafkaMessageEvent messageEvent, long threadId) {
		logger.info("[Thread-" + threadId + "]");
		notifyListeners(messageEvent);
	}
}
