package net.tinybrick.integration.message.unit;

import java.io.UnsupportedEncodingException;
import java.util.UUID;

import kafka.message.MessageAndMetadata;

import net.tinybrick.integration.kafka.KafkaConsumer;
import net.tinybrick.integration.kafka.event.KafkaMessageHandler;
import net.tinybrick.integration.message.IMessageConsumer;
import net.tinybrick.integration.message.IMessageConsumerFactory;
import net.tinybrick.integration.message.MessageEventListener;
import net.tinybrick.integration.message.configuration.KafkaConfigure;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import net.tinybrick.integration.kafka.event.KafkaMessageHandler.KafkaMessageEvent;
import net.tinybrick.integration.message.IMessageProducer;
import net.tinybrick.integration.message.IMessageProducerFactory;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes = KafkaConfigure.class)
public class TestKafkaMessageServer {
	@Autowired IMessageProducerFactory producerFactory;
	@Autowired
	IMessageConsumerFactory<KafkaMessageEvent> consumerFactory;

	IMessageProducer producer;
	IMessageConsumer consumer;

	@Before
	public void before() {
		producer = producerFactory.generate();

		consumer = consumerFactory.generateConsumer("test", new MessageEventListener<KafkaMessageEvent>() {
			public void receiveEvent(KafkaMessageHandler.KafkaMessageEvent event) {
				MessageAndMetadata<byte[], byte[]> messageAndMetadata = event.getEvent();

				try {
					String message = new String(messageAndMetadata.message(), "UTF-8");
					if (null != messageAndMetadata.key()) {
						System.out.println("[Partition-" + messageAndMetadata.partition() + "]" + "[Key-"
								+ new String(messageAndMetadata.key(), "UTF-8") + "]" + message);
					}
					else {
						System.out.println("[Partition-" + messageAndMetadata.partition() + "]" + "[Key-null]"
								+ message);
					}
				}
				catch (UnsupportedEncodingException e) {
					e.printStackTrace();
				}
			}
		}, 3, "test");
	}

	@After
	public void after() {
		if (null != consumer)
			((KafkaConsumer) consumer).shutdown();
	}

	@Test
	public void testMessage() throws InterruptedException {
		producer.publish("test", "Message ID: " + UUID.randomUUID().toString());
		Thread.sleep(10000);
	}
}
