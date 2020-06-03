package ru.prolib.caelum.restapi;

import java.io.IOException;
import java.io.OutputStream;
import java.time.Instant;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.StreamingOutput;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;

import ru.prolib.caelum.core.LBOHLCVMutable;

public class JsonOHLCVStreamer implements StreamingOutput {
	private final JsonFactory jsonFactory;
	private final ReadOnlyWindowStore<String, LBOHLCVMutable> store;
	private final String symbol, period;
	private final Long from, to, limit;
	private final ValueFormatter formatter;
	
	public JsonOHLCVStreamer(JsonFactory factory,
			ReadOnlyWindowStore<String, LBOHLCVMutable> store,
			String symbol, String period, Long from, Long to, Long limit)
	{
		this.jsonFactory = factory;
		this.store = store;
		this.symbol = symbol;
		this.period = period;
		this.from = from;
		this.to = to;
		this.limit = limit;
		this.formatter = new ValueFormatter();
	}

	@Override
	public void write(OutputStream output) throws IOException, WebApplicationException {
		JsonGenerator gen = jsonFactory.createGenerator(output);
		//gen.useDefaultPrettyPrinter();
		try {
			try ( WindowStoreIterator<LBOHLCVMutable> it = store.fetch(symbol,
					Instant.ofEpochMilli(from), Instant.ofEpochMilli(to)) )
			{
				gen.writeStartObject();
				gen.writeFieldName("time");		gen.writeNumber(System.currentTimeMillis());
				gen.writeFieldName("error");	gen.writeBoolean(false);
				gen.writeFieldName("code");		gen.writeNumber(0);
				gen.writeFieldName("message");	gen.writeNull();
				gen.writeFieldName("data");
				gen.writeStartObject();
				gen.writeFieldName("symbol");	gen.writeString(symbol);
				gen.writeFieldName("period");	gen.writeString(period);
				gen.writeFieldName("from");		gen.writeNumber(from);
				gen.writeFieldName("to");		gen.writeNumber(to);
				gen.writeFieldName("limit");	gen.writeNumber(limit);
				gen.writeFieldName("format");	gen.writeString("norm");
				gen.writeFieldName("rows");
				gen.writeStartArray();
				
				long total = 0;
				while ( it.hasNext() && total < limit ) {
					KeyValue<Long, LBOHLCVMutable> item = it.next();
					gen.writeStartArray();
					gen.writeNumber(item.key);
					LBOHLCVMutable v = item.value;
					int price_decimals = v.getPriceDecimals();
					gen.writeString(formatter.format(v.getOpenPrice(), price_decimals));
					gen.writeString(formatter.format(v.getHighPrice(), price_decimals));
					gen.writeString(formatter.format(v.getLowPrice(), price_decimals));
					gen.writeString(formatter.format(v.getClosePrice(), price_decimals));
					gen.writeString(formatter.format(v.getVolume(), v.getBigVolume(), v.getVolumeDecimals()));
					gen.writeEndArray();
					total ++;
				}
				
				gen.writeEndArray(); // end of rows
				gen.writeEndObject(); // end of data
				gen.writeEndObject(); // end of envelope
			}
		} finally {
			gen.close();
		}
	}

}
