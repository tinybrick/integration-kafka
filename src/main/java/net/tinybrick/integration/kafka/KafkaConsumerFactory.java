package net.tinybrick.integration.kafka;

import java.util.Properties;

import net.tinybrick.integration.kafka.event.KafkaMessageHandler.KafkaMessageEvent;
import net.tinybrick.integration.message.IMessageConsumer;
import net.tinybrick.integration.message.IMessageConsumerFactory;
import net.tinybrick.integration.message.MessageEventListener;

public class KafkaConsumerFactory implements IMessageConsumerFactory<KafkaMessageEvent> {
	Properties producerProps;

	public KafkaConsumerFactory(Properties producerProps) {
		this.producerProps = producerProps;
	}

	/* (non-Javadoc)
	 * @see com.wang.integration.message.IMessageConsumerFactory#generateConsumer(java.lang.String, com.wang.integration.kafka.event.MessageHandler.MessageEventListener)
	 */
	@Override
	public IMessageConsumer generateConsumer(String topic, MessageEventListener<KafkaMessageEvent> listener) {
		return generateConsumer(topic, listener, 1, null);
	}

	/* (non-Javadoc)
	 * @see com.wang.integration.message.IMessageConsumerFactory#generateConsumer(java.lang.String, com.wang.integration.kafka.event.MessageHandler.MessageEventListener, int)
	 */
	@Override
	public IMessageConsumer generateConsumer(String topic, MessageEventListener<KafkaMessageEvent> listener, int numThreads) {
		return generateConsumer(topic, listener, numThreads, null);
	}

	/* (non-Javadoc)
	 * @see com.wang.integration.message.IMessageConsumerFactory#generateConsumer(java.lang.String, com.wang.integration.kafka.event.MessageHandler.MessageEventListener, int, java.lang.String)
	 */
	@Override
	public IMessageConsumer generateConsumer(String topic, MessageEventListener<KafkaMessageEvent> listener, int numThreads,
			String group) {
		KafkaConsumer consumer = new KafkaConsumer(producerProps, group);
		consumer.setNumThreads(numThreads);
		consumer.addMessageListener(listener);
		consumer.subscribe(topic);

		return consumer;
	}
}
