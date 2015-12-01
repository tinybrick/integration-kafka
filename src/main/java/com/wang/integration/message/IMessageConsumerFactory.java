package com.wang.integration.message;

public interface IMessageConsumerFactory<T> {

	public abstract IMessageConsumer generateConsumer(String topic, MessageEventListener<T> listener);

	public abstract IMessageConsumer generateConsumer(String topic, MessageEventListener<T> listener, int numThreads);

	public abstract IMessageConsumer generateConsumer(String topic, MessageEventListener<T> listener, int numThreads,
			String group);

}