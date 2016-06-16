package net.tinybrick.integration.kafka;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

import net.tinybrick.integration.message.IMessageProducer;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/**
 * Hello world!
 *
 */
public class KafkaProducer implements IMessageProducer 
{
	int key = 0;
	private Producer<String, String> producer = null;

	public KafkaProducer(Properties props) {
		ProducerConfig config = new ProducerConfig(props);
		producer = new Producer<String, String>(config);
	}

	/* (non-Javadoc)
	 * @see com.wang.integration.kafka.IMessageProducer#publish(java.lang.String, java.lang.String)
	 */
	@Override
	public void publish(String topic, String message)
	{
		SimpleDateFormat sdf = new SimpleDateFormat("[yy-MM-dd/hh:mm:ss]");

		key++;
		String messageStr = new String(sdf.format(new Date()) + ": " + message);
		KeyedMessage<String, String> keyedMessage = new KeyedMessage<String, String>(topic, String.valueOf(key), messageStr);
		producer.send(keyedMessage);
	}
}
