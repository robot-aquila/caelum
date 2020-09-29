package ru.prolib.caelum.backnode.mvc;

import java.time.Clock;

import javax.ws.rs.core.StreamingOutput;

import com.fasterxml.jackson.core.JsonFactory;

import ru.prolib.caelum.backnode.ValueFormatter;
import ru.prolib.caelum.lib.Events;
import ru.prolib.caelum.lib.ICloseableIterator;
import ru.prolib.caelum.lib.ITuple;
import ru.prolib.caelum.service.AggregatedDataRequest;
import ru.prolib.caelum.service.EventListRequest;
import ru.prolib.caelum.service.IItemDataRequest;
import ru.prolib.caelum.service.IItemIterator;
import ru.prolib.caelum.service.SymbolListRequest;

public class StreamFactory {
	private final JsonFactory jsonFactory;
	private final ValueFormatter formatter;
	private final Clock clock;
	
	public StreamFactory(JsonFactory jsonFactory, ValueFormatter formatter, Clock clock) {
		this.jsonFactory = jsonFactory;
		this.formatter = formatter;
		this.clock = clock;
	}
	
	public StreamFactory() {
		this(new JsonFactory(), ValueFormatter.getInstance(), Clock.systemUTC());
	}
	
	public JsonFactory getJsonFactory() {
		return jsonFactory;
	}
	
	public ValueFormatter getFormatter() {
		return formatter;
	}
	
	public Clock getClock() {
		return clock;
	}
	
	public StreamingOutput categoriesToJson(ICloseableIterator<String> iterator) {
		return new StreamCategoriesToJson(jsonFactory, iterator, clock);
	}
	
	public StreamingOutput symbolsToJson(ICloseableIterator<String> iterator, SymbolListRequest request) {
		return new StreamSymbolsToJson(jsonFactory, iterator, request, clock);
	}
	
	public StreamingOutput itemsToJson(IItemIterator iterator, IItemDataRequest request) {
		return new StreamItemsToJson(jsonFactory, iterator, request, formatter, clock);
	}
	
	public StreamingOutput tuplesToJson(ICloseableIterator<ITuple> iterator, AggregatedDataRequest request) {
		return new StreamTuplesToJson(jsonFactory, iterator, request, formatter, clock);
	}
	
	public StreamingOutput eventsToJson(ICloseableIterator<Events> iterator, EventListRequest request) {
		return new StreamEventsToJson(jsonFactory, iterator, request, clock);
	}
	
	public StreamingOutput stringsToJson(ICloseableIterator<String> iterator) {
		return new StreamStringsToJson(jsonFactory, iterator, clock);
	}

}
