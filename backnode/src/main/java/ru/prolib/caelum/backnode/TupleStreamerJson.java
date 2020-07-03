package ru.prolib.caelum.backnode;

import java.io.IOException;
import java.io.OutputStream;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.StreamingOutput;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;

import ru.prolib.caelum.aggregator.AggregatedDataRequest;
import ru.prolib.caelum.core.ICloseableIterator;
import ru.prolib.caelum.core.ITuple;

public class TupleStreamerJson implements StreamingOutput {
	private final JsonFactory jsonFactory;
	private final ICloseableIterator<ITuple> iterator;
	private final AggregatedDataRequest request;
	private final ValueFormatter formatter;
	
	public TupleStreamerJson(JsonFactory factory,
			ICloseableIterator<ITuple> iterator,
			AggregatedDataRequest request,
			ValueFormatter formatter)
	{
		this.jsonFactory = factory;
		this.iterator = iterator;
		this.request = request;
		this.formatter = formatter;
	}
	
	public TupleStreamerJson(JsonFactory factory,
			ICloseableIterator<ITuple> iterator,
			AggregatedDataRequest request)
	{
		this(factory, iterator, request, new ValueFormatter());
	}

	@Override
	public void write(OutputStream output) throws IOException, WebApplicationException {
		JsonGenerator gen = jsonFactory.createGenerator(output);
		//gen.useDefaultPrettyPrinter();
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
				gen.writeFieldName("period");	gen.writeString(request.getPeriod().toString());
				gen.writeFieldName("from");		gen.writeNumber(request.getFrom());
				gen.writeFieldName("to");		gen.writeNumber(request.getTo());
				gen.writeFieldName("limit");	gen.writeNumber(request.getLimit());
				gen.writeFieldName("format");	gen.writeString("std");
				gen.writeFieldName("rows");
				gen.writeStartArray();
				
				long total = 0, limit = request.getLimit();
				while ( iterator.hasNext() && total < limit ) {
					ITuple tuple = iterator.next();
					gen.writeStartArray();
					gen.writeNumber(tuple.getTime());
					int decimals = tuple.getDecimals();
					gen.writeString(formatter.format(tuple.getOpen(), decimals));
					gen.writeString(formatter.format(tuple.getHigh(), decimals));
					gen.writeString(formatter.format(tuple.getLow(), decimals));
					gen.writeString(formatter.format(tuple.getClose(), decimals));
					gen.writeString(formatter.format(tuple.getVolume(), tuple.getBigVolume(),
							tuple.getVolumeDecimals()));
					gen.writeEndArray();
					total ++;
				}
				
				gen.writeEndArray(); // end of rows
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
