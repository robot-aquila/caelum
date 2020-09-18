package ru.prolib.caelum.lib;

import java.util.HashMap;
import java.util.Map;

public class EventsBuilder {
	private String symbol;
	private Long time;
	private Map<Integer, String> events;
	
	public EventsBuilder() {
		events = new HashMap<>();
	}
	
	public EventsBuilder withSymbol(String symbol) {
		this.symbol = symbol;
		return this;
	}
	
	public EventsBuilder withTime(long time) {
		this.time = time;
		return this;
	}
	
	public EventsBuilder withEvent(int event_id, String event_data) {
		this.events.put(event_id, event_data);
		return this;
	}
	
	public Events build() {
		if ( symbol == null ) {
			throw new IllegalStateException("Symbol was not defined");
		}
		if ( time == null ) {
			throw new IllegalStateException("Time was not defined");
		}
		if ( events.size() == 0 ) {
			throw new IllegalStateException("No events defined");
		}
		return new Events(symbol, time, new HashMap<>(events));
	}
	
	public boolean hasEvents() {
		return events.size() > 0;
	}

}
