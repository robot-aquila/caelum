package ru.prolib.caelum.core.dmx;

import java.util.LinkedList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageDispatcher<Subscriber, Payload> implements IMessageDispatcher<Subscriber, Payload> {
	private static final Logger logger = LoggerFactory.getLogger(MessageDispatcher.class);
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private static final Subscription END = new Subscription(null, null);
	
	private volatile LinkedNode<Message<Subscriber, Payload>> lastNode;
	private final AtomicBoolean closed;
	private final BlockingQueue<Subscription<Subscriber, Payload>> queue;
	private final Lock lock;
	private final Condition messageArrived;
	
	MessageDispatcher(LinkedNode<Message<Subscriber, Payload>> lastNode,
			AtomicBoolean closed,
			BlockingQueue<Subscription<Subscriber, Payload>> queue,
			Lock lock,
			Condition messageArrived)
	{
		this.lastNode = lastNode;
		this.closed = closed;
		this.queue = queue;
		this.lock = lock;
		this.messageArrived = messageArrived;
	}
	
	MessageDispatcher(LinkedNode<Message<Subscriber, Payload>> lastNode,
			AtomicBoolean closed,
			BlockingQueue<Subscription<Subscriber, Payload>> queue,
			Lock lock)
	{
		this(lastNode, closed, queue, lock, lock.newCondition());
	}
	
	public MessageDispatcher() {
		this(new LinkedNode<>(new Message<>()), new AtomicBoolean(), new LinkedBlockingQueue<>(), new ReentrantLock());
	}

	@Override
	public ISubscription<Subscriber> register(Subscriber subscriber) {
		if ( closed.get() ) {
			throw new IllegalStateException();
		}
		Subscription<Subscriber, Payload> subscription = new Subscription<>(subscriber, lastNode);
		// Between entering this method and this point a new message may arrive.
		// So all new subscribers go to active pool.
		queue.add(subscription);
		return subscription;
	}

	@Override
	public void send(Payload payload, ISubscription<Subscriber> subscription) {
		if ( closed.get() ) {
			throw new IllegalStateException();
		}
		lock.lock();
		try {
			lastNode = lastNode.append(new Message<>(subscription, payload));
			messageArrived.signalAll();
		} finally {
			lock.unlock();
		}
	}

	@Override
	public void broadcast(Payload payload) {
		send(payload, null);
	}
	
	/**
	 * Get next message to dispatch.
	 * <p>
	 * This method must be called from sending thread. It will cause lock if no
	 * message currently available to dispatch. The waiting ends after new message
	 * arrived or dispatcher closed.
	 * <p>
	 * All messages obtained by calling this method must be passed to {@link #sent(Message)}
	 * after processing. This causes subscription reschedule and make it possible to receive next messages
	 * for that subscriber. Omitting {@link #sent(Message)} call will cause subscription loss. 
	 * <p>
	 * @return message or null if dispatcher closed and no more messages will arrive
	 */
	@Override
	public Message<Subscriber, Payload> next() {
		while ( closed.get() == false ) {
			Subscription<Subscriber, Payload> s;
			try {
				s = queue.take();
			} catch ( InterruptedException e ) {
				logger.error("Unexpected interruption (ignore): ", e);
				continue;
			}
			if ( s == END ) {
				queue.add(s); // Reschedule for others. Forever...
				return null;
			}
			
			Message<Subscriber, Payload> msg = s.nextOrNull();
			if ( msg != null ) {
				return msg;
			}
			
			// No more messages. Next, start working in critical section to avoid new messages for passed subscribers.
			lock.lock();
			try {
				// First, test subscriber again. It is possible that new message appeared since last check.
				msg = s.nextOrNull();
				if ( msg != null ) {
					return msg;
				}
				
				LinkedList<Subscription<Subscriber, Payload>> cache = new LinkedList<>();
				cache.add(s);
				do {
					s = queue.poll();
					if ( s == END ) {
						queue.add(s);
						return null;
					}
					if ( s == null ) {
						break;
					}

					msg = s.nextOrNull();
					if ( msg != null ) {
						break;
					}
					cache.add(s);
				} while ( s != null );
				
				// It is regular case. Reschedule.
				cache.stream().forEach(x -> queue.add(x));
				if ( msg != null ) {
					return msg;
				} else {
					messageArrived.awaitUninterruptibly();
				}
			} finally {
				lock.unlock();
			}
		}
		return null;
	}

	@Override
	@SuppressWarnings({ "unchecked" })
	public void processed(Message<Subscriber, Payload> msg) {
		// We do not know are there more messages or not.
		// Just schedule it to make it clear.
		queue.add((Subscription<Subscriber, Payload>) msg.getSubscription());
		// It's OK that other threads may sit and wait for arrival signal.
		// At least this thread take care of that subscriber.
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public void close() {
		if ( closed.compareAndSet(false, true) ) {
			queue.add(END);
			lock.lock();
			try {
				messageArrived.signalAll();
			} finally {
				lock.unlock();
			}
		}
	}
	
}
