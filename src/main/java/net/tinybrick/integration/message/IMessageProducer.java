package net.tinybrick.integration.message;

public interface IMessageProducer {
	public abstract void publish(String topic, String message);

}