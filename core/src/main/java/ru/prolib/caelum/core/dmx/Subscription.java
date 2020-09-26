package ru.prolib.caelum.core.dmx;

import java.util.ConcurrentModificationException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class Subscription<Subscriber, Payload> implements ISubscription<Subscriber> {
	
	private final Subscriber subscriber;
	/**
	 * The last sent message is only used to determine a next message in the chain.
	 */
	private final AtomicReference<LinkedNode<Message<Subscriber, Payload>>> last;
	
	/**
	 * The marker of that this subscription is closed.
	 */
	private final AtomicBoolean closed;
	
	Subscription(Subscriber subscriber,
			AtomicReference<LinkedNode<Message<Subscriber, Payload>>> last,
			AtomicBoolean closed)
	{
		this.subscriber = subscriber;
		this.last = last;
		this.closed = closed;
	}
	
	public Subscription(Subscriber subscriber, LinkedNode<Message<Subscriber, Payload>> last) {
		this(subscriber, new AtomicReference<>(last), new AtomicBoolean());
	}

	@Override
	public Subscriber getSubscriber() {
		return subscriber;
	}

	@Override
	public boolean isClosed() {
		return closed.get();
	}

	@Override
	public void close() {
		closed.compareAndSet(false, true);
	}
	
	/**
	 * Get next message to dispatch.
	 * <p>
	 * @return next message
	 */
	public Message<Subscriber, Payload> nextOrNull() {
		if ( closed.get() ) {
			throw new IllegalStateException();
		}
		Message<Subscriber, Payload> nextMessage = null;
		LinkedNode<Message<Subscriber, Payload>> lastNode, nextNode;
		do {
			lastNode = last.get();
			nextNode = lastNode.nextOrNull();
			if ( nextNode == null ) {
				return null;
			}
			if ( ! last.compareAndSet(lastNode, nextNode) ) {
				throw new ConcurrentModificationException();
			}
			nextMessage = nextNode.getPayload();
		} while ( nextMessage.getSubscription() != null && nextMessage.getSubscription() != this );
		return nextMessage.getSubscription() == this ? nextMessage : new Message<>(this, nextMessage.getPayload());
	}
	
	public LinkedNode<Message<Subscriber, Payload>> getLastNode() {
		return last.get();
	}

}
