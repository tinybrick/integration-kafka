package com.wang.integration.message;

public interface IMessageProducer {
	public abstract void publish(String topic, String message);

}