package ru.prolib.caelum.core.dmx;

public interface ISubscription<Subscriber> {
	
	/**
	 * Get subscriber definition of this subscription.
	 * <p>
	 * @return subscriber
	 */
	Subscriber getSubscriber();

	/**
	 * Test that subscription is closed.
	 * <p>
	 * @return true if closed, false otherwise
	 */
	boolean isClosed();
	
	/**
	 * Close subscription.
	 * <p>
	 * <b>Warning!</b> All scheduled messages will lost.
	 */
	void close();

}
