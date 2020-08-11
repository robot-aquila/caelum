package ru.prolib.caelum.backnode.mvc;

import java.io.IOException;
import java.io.OutputStream;
import java.time.Clock;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.StreamingOutput;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;

import ru.prolib.caelum.core.ICloseableIterator;

public class StreamStringsToJson implements StreamingOutput {
	private final JsonFactory jsonFactory;
	private final ICloseableIterator<String> iterator;
	private final Clock clock;
	
	public StreamStringsToJson(JsonFactory jsonFactory, ICloseableIterator<String> iterator, Clock clock) {
		this.jsonFactory = jsonFactory;
		this.iterator = iterator;
		this.clock = clock;
	}
	
	public JsonFactory getJsonFactory() {
		return jsonFactory;
	}
	
	public ICloseableIterator<String> getIterator() {
		return iterator;
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
			gen.writeFieldName("rows");
			gen.writeStartArray();
			while ( iterator.hasNext() ) {
				gen.writeString(iterator.next());
			}
			gen.writeEndArray(); // end of row
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
