package ru.prolib.caelum.backnode.mvc;

import java.io.IOException;
import java.io.OutputStream;
import java.time.Clock;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.StreamingOutput;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;

import ru.prolib.caelum.core.ICloseableIterator;
import ru.prolib.caelum.lib.Events;
import ru.prolib.caelum.symboldb.EventListRequest;

public class StreamEventsToJson implements StreamingOutput {
	private final JsonFactory jsonFactory;
	private final ICloseableIterator<Events> iterator;
	private final EventListRequest request;
	private final Clock clock;
	
	public StreamEventsToJson(JsonFactory jsonFactory,
			ICloseableIterator<Events> iterator,
			EventListRequest request,
			Clock clock)
	{
		this.jsonFactory = jsonFactory;
		this.iterator = iterator;
		this.request = request;
		this.clock = clock;
	}
	
	public JsonFactory getJsonFactory() {
		return jsonFactory;
	}
	
	public ICloseableIterator<Events> getIterator() {
		return iterator;
	}
	
	public EventListRequest getRequest() {
		return request;
	}
	
	public Clock getClock() {
		return clock;
	}
	
	@Override
	public void write(OutputStream output) throws IOException, WebApplicationException {
		JsonGenerator gen = jsonFactory.createGenerator(output);
		gen.useDefaultPrettyPrinter();
		try {
			gen.writeStartObject();
			gen.writeFieldName("time");		gen.writeNumber(clock.millis());
			gen.writeFieldName("error");	gen.writeBoolean(false);
			gen.writeFieldName("code");		gen.writeNumber(0);
			gen.writeFieldName("message");	gen.writeNull();
			gen.writeFieldName("data");
			gen.writeStartObject();
			gen.writeFieldName("symbol");	gen.writeString(request.getSymbol());
			gen.writeFieldName("rows");
			gen.writeStartArray();

			while ( iterator.hasNext() ) {
				Events e = iterator.next();
				gen.writeStartObject();
				gen.writeFieldName("time");		gen.writeNumber(e.getTime());
				gen.writeFieldName("events");
				gen.writeStartObject();
				for ( int event_id : e.getEventIDs() ) {
					gen.writeFieldName(Integer.toString(event_id));
					gen.writeString(e.getEvent(event_id));
				}
				gen.writeEndObject();
				gen.writeEndObject();
			}
			
			gen.writeEndArray(); // end of rows
			gen.writeEndObject(); // end of data
			gen.writeEndObject(); // end of envelope
		} finally {
			gen.close();
			try {
				iterator.close();
			} catch ( Exception e ) {
				throw new IOException(e);
			}
		}
	}

}
