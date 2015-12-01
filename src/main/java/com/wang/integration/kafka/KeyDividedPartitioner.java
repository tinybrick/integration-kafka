package com.wang.integration.kafka;

import kafka.producer.DefaultPartitioner;
import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

/**
 * In this example, "test" topic has 3 partitions, we want messages has been spreaded by key value
 */
public class KeyDividedPartitioner extends DefaultPartitioner implements Partitioner {
	public KeyDividedPartitioner(VerifiableProperties props) {
		super(props);
	}

	public int partition(Object key, int numPartitions) {
		int partition;
		partition = Integer.valueOf((String) key).intValue() % numPartitions;
		return partition;
	}
}
