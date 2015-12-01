package com.wang.integration.kafka.event;

import java.util.Collection;
import java.util.EventObject;
import java.util.HashSet;
import java.util.Iterator;

import com.wang.integration.message.MessageEventListener;

import kafka.message.MessageAndMetadata;

public class KafkaMessageHandler {
	private Collection<MessageEventListener<KafkaMessageEvent>> listeners;

	public void addMessageListener(MessageEventListener<KafkaMessageEvent> listener) {
		if (listeners == null) {
			listeners = new HashSet<MessageEventListener<KafkaMessageEvent>>();
		}
		listeners.add(listener);
	}

	public void removeMessageListener(MessageEventListener<KafkaMessageEvent> listener) {
		if (listeners == null)
			return;
		listeners.remove(listener);
	}

	protected void notifyListeners(KafkaMessageEvent event) {
		Iterator<MessageEventListener<KafkaMessageEvent>> iter = listeners.iterator();
		while (iter.hasNext()) {
			MessageEventListener<KafkaMessageEvent> listener = (MessageEventListener<KafkaMessageEvent>) iter.next();
			listener.receiveEvent(event);
		}
	}

	public class KafkaMessageEvent extends EventObject {
		private static final long serialVersionUID = 7058797155181337370L;
		MessageAndMetadata<byte[], byte[]> messageAndMetadata;

		public KafkaMessageEvent(MessageAndMetadata<byte[], byte[]> source) {
			super(source);
			messageAndMetadata = source;
		}

		public MessageAndMetadata<byte[], byte[]> getEvent() {
			return messageAndMetadata;
		}
	}
}
