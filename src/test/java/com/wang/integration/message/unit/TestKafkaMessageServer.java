package com.wang.integration.message.unit;

import java.io.UnsupportedEncodingException;
import java.util.UUID;

import kafka.message.MessageAndMetadata;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.wang.integration.kafka.KafkaConsumer;
import com.wang.integration.kafka.event.KafkaMessageHandler.KafkaMessageEvent;
import com.wang.integration.message.IMessageConsumer;
import com.wang.integration.message.IMessageConsumerFactory;
import com.wang.integration.message.IMessageProducer;
import com.wang.integration.message.IMessageProducerFactory;
import com.wang.integration.message.MessageEventListener;
import com.wang.integration.message.configuration.KafkaConfigure;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes = KafkaConfigure.class)
public class TestKafkaMessageServer {
	@Autowired IMessageProducerFactory producerFactory;
	@Autowired IMessageConsumerFactory<KafkaMessageEvent> consumerFactory;

	IMessageProducer producer;
	IMessageConsumer consumer;

	@Before
	public void before() {
		producer = producerFactory.generate();

		consumer = consumerFactory.generateConsumer("test", new MessageEventListener<KafkaMessageEvent>() {
			public void receiveEvent(KafkaMessageEvent event) {
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
