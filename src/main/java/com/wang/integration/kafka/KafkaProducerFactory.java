package com.wang.integration.kafka;

import java.util.Properties;

import com.wang.integration.message.IMessageProducer;
import com.wang.integration.message.IMessageProducerFactory;

public class KafkaProducerFactory implements IMessageProducerFactory {
	Properties producerProps;

	public KafkaProducerFactory(Properties producerProps) {
		this.producerProps = producerProps;
	}

	/* (non-Javadoc)
	 * @see com.wang.integration.message.IMessageProducerFactory#generate()
	 */
	@Override
	public IMessageProducer generate() {
		KafkaProducer kafkaProducer = new KafkaProducer(producerProps);
		return kafkaProducer;
	}
}
