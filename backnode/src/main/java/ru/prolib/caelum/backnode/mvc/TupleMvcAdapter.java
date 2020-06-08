package ru.prolib.caelum.backnode.mvc;

import java.time.Instant;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.kafka.streams.KeyValue;

import ru.prolib.caelum.backnode.ValueFormatter;
import ru.prolib.caelum.core.Tuple;

public class TupleMvcAdapter extends KeyValue<Long, Tuple> {
	private final ValueFormatter formatter;
	
	public TupleMvcAdapter(Long key, Tuple value, ValueFormatter formatter) {
		super(key, value);
		this.formatter = formatter;
	}
	
	public TupleMvcAdapter(Long key, Tuple value) {
		this(key, value, ValueFormatter.getInstance());
	}
	
	public Long getKey() {
		return key;
	}
	
	public Tuple getValue() {
		return value;
	}
	
	@Override
	public int hashCode() {
		return new HashCodeBuilder(1451200203, 59)
				.append(key)
				.append(value)
				.append(formatter)
				.build();
	}
	
	@Override
	public boolean equals(Object other) {
		if ( other == this ) {
			return true;
		}
		if ( other == null || other.getClass() != TupleMvcAdapter.class ) {
			return false;
		}
		TupleMvcAdapter o = (TupleMvcAdapter) other;
		return new EqualsBuilder()
				.append(o.key, key)
				.append(o.value, value)
				.append(o.formatter, formatter)
				.build();
	}
	
	public String getTime() {
		return Instant.ofEpochMilli(key).toString();
	}
	
	public String getOpen() {
		return formatter.format(value.open, value.decimals);
	}
	
	public String getHigh() {
		return formatter.format(value.high, value.decimals);
	}
	
	public String getLow() {
		return formatter.format(value.low, value.decimals);
	}
	
	public String getClose() {
		return formatter.format(value.close, value.decimals);
	}
	
	public String getVolume() {
		return formatter.format(value.volume, value.bigVolume, value.volDecimals);
	}

}
