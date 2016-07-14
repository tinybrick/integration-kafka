package net.tinybrick.integration.message;

import java.util.EventListener;

public interface MessageEventListener<T> extends EventListener {
	public void receiveEvent(T event);
}
