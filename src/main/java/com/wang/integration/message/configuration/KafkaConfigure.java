package com.wang.integration.message.configuration;

import java.util.Properties;

import kafka.serializer.StringEncoder;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;

import com.wang.integration.kafka.KafkaConsumerFactory;
import com.wang.integration.kafka.KafkaProducerFactory;
import com.wang.integration.kafka.KeyDividedPartitioner;
import com.wang.integration.kafka.event.KafkaMessageHandler.KafkaMessageEvent;
import com.wang.integration.message.IMessageConsumerFactory;
import com.wang.integration.message.IMessageProducerFactory;

@Configuration
@ComponentScan
@EnableConfigurationProperties({ PropertySourcesPlaceholderConfigurer.class })
@PropertySource(value = "classpath:config/kafka.properties")
public class KafkaConfigure {
	protected static final Class<StringEncoder> serializer_encode_class = StringEncoder.class;
	protected static final Class<KeyDividedPartitioner> serializer_partitioner_class = KeyDividedPartitioner.class;

	@Value("${zookeeper.connection}") String zookeeper_connection;

	private static final String zk_sessiontimeout_ms = "2000";
	private static final String zk_synctime_ms = "200";
	private static final String zk_autocommit_interval_ms = "1000";

	@Bean
	public IMessageConsumerFactory<KafkaMessageEvent> kafkaConsumerFactory() {
		Properties consumerProps = new Properties();
		consumerProps.put("zookeeper.connect", zookeeper_connection);
		consumerProps.put("zookeeper.session.timeout.ms", zk_sessiontimeout_ms);
		consumerProps.put("zookeeper.sync.time.ms", zk_synctime_ms);
		consumerProps.put("auto.commit.interval.ms", zk_autocommit_interval_ms);

		return new KafkaConsumerFactory(consumerProps);
	}

	@Value("${kafka.broker.list}") String kafka_broker_list;
	@Value("${kafka.request.required.acks:1}") String kafka_request_required_acks = "1";

	@Bean
	public IMessageProducerFactory kafkaProducerFactory() {
		Properties producerProps = new Properties();
		producerProps.put("metadata.broker.list", kafka_broker_list);
		producerProps.put("request.required.acks", kafka_request_required_acks);
		producerProps.put("serializer.class", serializer_encode_class.getName());
		producerProps.put("partitioner.class", serializer_partitioner_class.getName());

		return new KafkaProducerFactory(producerProps);
	}
}
