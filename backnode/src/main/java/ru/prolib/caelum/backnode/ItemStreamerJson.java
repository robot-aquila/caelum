package ru.prolib.caelum.backnode;

import java.io.IOException;
import java.io.OutputStream;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.StreamingOutput;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;

import ru.prolib.caelum.core.Item;
import ru.prolib.caelum.itemdb.IItemData;
import ru.prolib.caelum.itemdb.IItemDataIterator;
import ru.prolib.caelum.itemdb.IItemDataRequest;

public class ItemStreamerJson implements StreamingOutput {
	private final JsonFactory jsonFactory;
	private final IItemDataIterator iterator;
	private final IItemDataRequest request;
	private final ValueFormatter formatter;
	
	public ItemStreamerJson(JsonFactory factory,
			IItemDataIterator iterator,
			IItemDataRequest request,
			ValueFormatter formatter)
	{
		this.jsonFactory = factory;
		this.iterator = iterator;
		this.request = request;
		this.formatter = formatter;
	}
	
	public ItemStreamerJson(JsonFactory factory,
			IItemDataIterator iterator,
			IItemDataRequest request)
	{
		this(factory, iterator, request, ValueFormatter.getInstance());
	}

	@Override
	public void write(OutputStream output) throws IOException, WebApplicationException {
		JsonGenerator gen = jsonFactory.createGenerator(output);
		gen.useDefaultPrettyPrinter();
		try {
			try {
				gen.writeStartObject();
				gen.writeFieldName("time");		gen.writeNumber(System.currentTimeMillis());
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
				
				long total = 0, limit = request.getLimit(), last_offset = 0L;
				while ( iterator.hasNext() && total < limit ) {
					IItemData item_data = iterator.next();
					last_offset = item_data.getOffset();
					Item item = item_data.getItem();
					gen.writeStartArray();
					gen.writeNumber(item_data.getTime());
					gen.writeString(formatter.format(item.getValue(), item.getDecimals()));
					gen.writeString(formatter.format(item.getVolume(), item.getVolumeDecimals()));
					gen.writeEndArray();
				}
				
				gen.writeEndArray(); // end of rows
				gen.writeFieldName("magic");		gen.writeString(iterator.getMetaData().getMagic());
				gen.writeFieldName("from_offset");	gen.writeNumber(last_offset + 1);
				gen.writeEndObject(); // end of data
				gen.writeEndObject(); // end of envelope
			} finally {
				try {
					iterator.close();
				} catch ( Exception e ) {
					throw new IOException(e);
				}
			}
		} finally {
			gen.close();
		}
	}

}
