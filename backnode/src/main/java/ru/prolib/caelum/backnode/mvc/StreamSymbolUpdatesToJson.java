package ru.prolib.caelum.backnode.mvc;

import java.io.IOException;
import java.io.OutputStream;
import java.time.Clock;
import java.util.Iterator;
import java.util.Map;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.StreamingOutput;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;

import ru.prolib.caelum.core.ICloseableIterator;
import ru.prolib.caelum.symboldb.SymbolUpdate;

public class StreamSymbolUpdatesToJson implements StreamingOutput {
	private final JsonFactory jsonFactory;
	private final ICloseableIterator<SymbolUpdate> iterator;
	private final String symbol;
	private final Clock clock;
	
	public StreamSymbolUpdatesToJson(JsonFactory jsonFactory,
			ICloseableIterator<SymbolUpdate> iterator,
			String symbol,
			Clock clock)
	{
		this.jsonFactory = jsonFactory;
		this.iterator = iterator;
		this.symbol = symbol;
		this.clock = clock;
	}
	
	public JsonFactory getJsonFactory() {
		return jsonFactory;
	}
	
	public ICloseableIterator<SymbolUpdate> getIterator() {
		return iterator;
	}
	
	public String getRequest() {
		return symbol;
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
			gen.writeFieldName("symbol");	gen.writeString(symbol);
			gen.writeFieldName("rows");
			gen.writeStartArray();

			while ( iterator.hasNext() ) {
				SymbolUpdate update = iterator.next();
				gen.writeStartObject();
				gen.writeFieldName("time");		gen.writeNumber(update.getTime());
				gen.writeFieldName("tokens");
				gen.writeStartObject();
				Iterator<Map.Entry<Integer, String>> it = update.getTokens().entrySet().iterator();
				while ( it.hasNext() ) {
					Map.Entry<Integer, String> entry = it.next();
					gen.writeFieldName(Integer.toString(entry.getKey()));
					gen.writeString(entry.getValue());
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
