package ru.prolib.caelum.lib.dmx;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

public class Message<Subscriber, Payload> {
	private final ISubscription<Subscriber> subscription;
	private final Payload payload;
	
	public Message(ISubscription<Subscriber> subscription, Payload payload) {
		this.subscription = subscription;
		this.payload = payload;
	}
	
	/**
	 * Create zero-payload broadcast message.
	 */
	public Message() {
		this(null, null);
	}

	/**
	 * Get subscription handler.
	 * <p>
	 * @return subscription handler or null in case of broadcast message
	 */
	public ISubscription<Subscriber> getSubscription() {
		return subscription;
	}

	/**
	 * Get message recipient.
	 * <p>
	 * @return recipient-subscriber or null in case of broadcast message
	 */
	public Subscriber getSubscriber() {
		return subscription == null ? null : subscription.getSubscriber();
	}

	/**
	 * Get message payload.
	 * <p>
	 * @return payload
	 */
	public Payload getPayload() {
		return payload;
	}

	@Override
	public String toString() {
		return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
				.append("subscriber", getSubscriber())
				.append("payload", payload)
				.build();
	}
	
	@Override
	public boolean equals(Object other) {
		if ( other == this ) {
			return true;
		}
		if ( other == null || other.getClass() != Message.class ) {
			return false;
		}
		Message<?, ?> o = (Message<?, ?>) other;
		return new EqualsBuilder()
				.append(o.subscription, subscription)
				.append(o.payload, payload)
				.build();
	}

}
