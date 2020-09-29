package ru.prolib.caelum.backnode.mvc;

import java.io.IOException;
import java.io.OutputStream;
import java.time.Clock;
import java.util.NoSuchElementException;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.StreamingOutput;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;

import ru.prolib.caelum.backnode.ValueFormatter;
import ru.prolib.caelum.lib.IItem;
import ru.prolib.caelum.service.IItemDataRequest;
import ru.prolib.caelum.service.IItemIterator;
import ru.prolib.caelum.service.ItemDataResponse;

public class StreamItemsToJson implements StreamingOutput {
	private final JsonFactory jsonFactory;
	private final IItemIterator iterator;
	private final IItemDataRequest request;
	private final ValueFormatter formatter;
	private final Clock clock;
	
	public StreamItemsToJson(JsonFactory factory,
			IItemIterator iterator,
			IItemDataRequest request,
			ValueFormatter formatter,
			Clock clock)
	{
		this.jsonFactory = factory;
		this.iterator = iterator;
		this.request = request;
		this.formatter = formatter;
		this.clock = clock;
	}
	
	public JsonFactory getJsonFactory() {
		return jsonFactory;
	}
	
	public IItemIterator getIterator() {
		return iterator;
	}
	
	public IItemDataRequest getRequest() {
		return request;
	}
	
	public ValueFormatter getFormatter() {
		return formatter;
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
			gen.writeFieldName("format");	gen.writeString("std");
			gen.writeFieldName("rows");
			gen.writeStartArray();
			
			int total = 0, limit = request.getLimit() == null ? Integer.MAX_VALUE : request.getLimit();
			while ( iterator.hasNext() && total < limit ) {
				try {
					IItem item = iterator.next();
					gen.writeStartArray();
					gen.writeNumber(item.getTime());
					gen.writeString(formatter.format(item.getValue(), item.getDecimals()));
					gen.writeString(formatter.format(item.getVolume(), item.getVolumeDecimals()));
					gen.writeEndArray();
				} catch ( NoSuchElementException e ) {
					// Sometimes we will get that exception because of AK lag
				}
			}
			
			gen.writeEndArray(); // end of rows
			ItemDataResponse meta_data = iterator.getMetaData();
			gen.writeFieldName("magic");		gen.writeString(meta_data.getMagic());
			gen.writeFieldName("fromOffset");	gen.writeNumber(meta_data.getOffset() + 1);
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
