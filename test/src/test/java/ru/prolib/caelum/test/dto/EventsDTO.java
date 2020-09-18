package ru.prolib.caelum.test.dto;

import java.util.Map;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

public class EventsDTO {
	public Long time;
	public Map<Integer, String> events;
	
	public EventsDTO() {
		
	}
	
	public EventsDTO(long time, Map<Integer, String> tokens) {
		this.time = time;
		this.events = tokens;
	}
	
	@Override
	public boolean equals(Object other) {
		if ( other == this ) {
			return true;
		}
		if ( other == null || other.getClass() != EventsDTO.class ) {
			return false;
		}
		EventsDTO o = (EventsDTO) other;
		return new EqualsBuilder()
				.append(o.time, time)
				.append(o.events, events)
				.build();
	}
	
	@Override
	public int hashCode() {
		return new HashCodeBuilder(8290117, 79)
				.append(time)
				.append(events)
				.build();
	}
	
	@Override
	public String toString() {
		return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
				.append("time", time)
				.append("events", events)
				.build();
	}
	
}
