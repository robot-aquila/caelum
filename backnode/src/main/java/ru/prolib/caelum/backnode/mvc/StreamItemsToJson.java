package ru.prolib.caelum.backnode.mvc;

import java.io.IOException;
import java.io.OutputStream;
import java.time.Clock;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.StreamingOutput;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;

import ru.prolib.caelum.backnode.ValueFormatter;
import ru.prolib.caelum.core.IItem;
import ru.prolib.caelum.itemdb.IItemIterator;
import ru.prolib.caelum.itemdb.ItemDataResponse;
import ru.prolib.caelum.itemdb.IItemDataRequest;

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
			gen.writeFieldName("to");		gen.writeNumber(request.getTo());
			gen.writeFieldName("limit");	gen.writeNumber(request.getLimit());
			gen.writeFieldName("format");	gen.writeString("std");
			gen.writeFieldName("rows");
			gen.writeStartArray();
			
			long total = 0, limit = request.getLimit();
			while ( iterator.hasNext() && total < limit ) {
				IItem item = iterator.next();
				gen.writeStartArray();
				gen.writeNumber(item.getTime());
				gen.writeString(formatter.format(item.getValue(), item.getDecimals()));
				gen.writeString(formatter.format(item.getVolume(), item.getVolumeDecimals()));
				gen.writeEndArray();
			}
			
			gen.writeEndArray(); // end of rows
			ItemDataResponse meta_data = iterator.getMetaData();
			gen.writeFieldName("magic");		gen.writeString(meta_data.getMagic());
			gen.writeFieldName("from_offset");	gen.writeNumber(meta_data.getOffset() + 1);
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
