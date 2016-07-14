package net.tinybrick.integration.kafka;

import java.util.Properties;

import net.tinybrick.integration.message.IMessageProducer;
import net.tinybrick.integration.message.IMessageProducerFactory;

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
