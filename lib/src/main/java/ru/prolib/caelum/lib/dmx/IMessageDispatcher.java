package ru.prolib.caelum.lib.dmx;

public interface IMessageDispatcher<Subscriber, Payload> {

	/**
	 * New subscriber registration.
	 * <p>
	 * @param subscriber - subscriber definition
	 * @return subscription handler - object that address subscription and control it
	 */
	ISubscription<Subscriber> register(Subscriber subscriber);
	
	/**
	 * Schedule a broadcast message.
	 * <p>
	 * @param payload - message payload
	 */
	void broadcast(Payload payload);

	/**
	 * Schedule a private message for the subscriber.
	 * <p>
	 * @param payload - message payload
	 * @param subscription - recipient subscriber
	 */
	void send(Payload payload, ISubscription<Subscriber> subscription);
	
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
	Message<Subscriber, Payload> next();

	/**
	 * Mark message as sent and reschedule subscription to get next messages.
	 * <p>
	 * @param message - message previously obtained via {@link #next()} call.
	 */
	void processed(Message<Subscriber, Payload> msg);
	
	void close();

}
