package ru.prolib.caelum.service.symboldb;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

public class EventKey {
	private final String symbol;
	private final long time;
	private final int eventID;
	
	public EventKey(String symbol, long time, int event_id) {
		this.symbol = symbol;
		this.time = time;
		this.eventID = event_id;
	}
	
	public String getSymbol() {
		return symbol;
	}
	
	public long getTime() {
		return time;
	}
	
	public int getEventID() {
		return eventID;
	}
	
	@Override
	public String toString() {
		return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
				.append("symbol", symbol)
				.append("time", time)
				.append("eventID", eventID)
				.build();
	}
	
	@Override
	public int hashCode() {
		return new HashCodeBuilder(100235, 91)
				.append(symbol)
				.append(time)
				.append(eventID)
				.build();
	}
	
	@Override
	public boolean equals(Object other) {
		if ( other == this ) {
			return true;
		}
		if ( other == null || other.getClass() != EventKey.class ) {
			return false;
		}
		EventKey o = (EventKey) other;
		return new EqualsBuilder()
				.append(o.symbol, symbol)
				.append(o.time, time)
				.append(o.eventID, eventID)
				.build();
	}

}
