package com.wang.integration.kafka;

import java.util.Properties;

import com.wang.integration.kafka.event.KafkaMessageHandler.KafkaMessageEvent;
import com.wang.integration.message.IMessageConsumer;
import com.wang.integration.message.IMessageConsumerFactory;
import com.wang.integration.message.MessageEventListener;

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
