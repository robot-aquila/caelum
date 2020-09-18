package ru.prolib.caelum.lib;

import java.util.Collection;
import java.util.Map;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

public class Events {
	private final String symbol;
	private final long time;
	private final Map<Integer, String> events;
	
	public Events(String symbol, long time, Map<Integer, String> events) {
		this.symbol = symbol;
		this.time = time;
		this.events = events;
	}
	
	public String getSymbol() {
		return symbol;
	}
	
	public long getTime() {
		return time;
	}
	
	/**
	 * Test for event exist.
	 * <p>
	 * @param event_id - event ID
	 * @return true if such event has exist in this set, false otherwise
	 */
	public boolean hasEvent(int event_id) {
		return events.containsKey(event_id);
	}
	
	/**
	 * Test for deletion marker for an event.
	 * <p>
	 * @param event_id - event ID
	 * @return true if event with such ID should be deleted, false otherwise
	 */
	public boolean isEventDelete(int event_id) {
		return events.containsKey(event_id) && events.get(event_id) == null;
	}
	
	public String getEvent(int event_id) {
		return events.get(event_id);
	}
	
	public Collection<Integer> getEventIDs() {
		return events.keySet();
	}
	
	@Override
	public String toString() {
		return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
				.append("symbol", symbol)
				.append("time", time)
				.append("events", events)
				.build();
	}
	
	@Override
	public int hashCode() {
		return new HashCodeBuilder(7760129, 903)
				.append(symbol)
				.append(time)
				.append(events)
				.build();
	}
	
	@Override
	public boolean equals(Object other) {
		if ( other == this ) {
			return true;
		}
		if ( other == null || other.getClass() != Events.class ) {
			return false;
		}
		Events o = (Events) other;
		return new EqualsBuilder()
				.append(o.symbol, symbol)
				.append(o.time, time)
				.append(o.events, events)
				.build();
	}
	
}
