package ru.prolib.caelum.aggregator.kafka;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.kafka.streams.KafkaStreams;

public class KafkaAggregatorStatusInfo {
	private final String source, target, store;
	private final boolean availability;
	private final KafkaStreams.State state;
	
	public KafkaAggregatorStatusInfo(String source,
			String target,
			String store,
			boolean availability,
			KafkaStreams.State state)
	{
		this.source = source;
		this.target = target;
		this.store = store;
		this.availability = availability;
		this.state = state;
	}
	
	public String getSource() {
		return source;
	}
	
	public String getTarget() {
		return target;
	}
	
	public String getStore() {
		return store;
	}
	
	public boolean getAvailability() {
		return availability;
	}
	
	public KafkaStreams.State getState() {
		return state;
	}
	
	@Override
	public String toString() {
		return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
				.append("source", source)
				.append("target", target)
				.append("store", store)
				.append("availability", availability)
				.append("state", state)
				.build();
	}
	
	@Override
	public int hashCode() {
		return new HashCodeBuilder(78910263, 301)
				.append(source)
				.append(target)
				.append(store)
				.append(availability)
				.append(state)
				.build();
	}
	
	@Override
	public boolean equals(Object other) {
		if ( other == this ) {
			return true;
		}
		if ( other == null || other.getClass() != KafkaAggregatorStatusInfo.class ) {
			return false;
		}
		KafkaAggregatorStatusInfo o = (KafkaAggregatorStatusInfo) other;
		return new EqualsBuilder()
				.append(o.source, source)
				.append(o.target, target)
				.append(o.store, store)
				.append(o.availability, availability)
				.append(o.state, state)
				.build();
	}

}
