package ru.prolib.caelum.backnode.mvc;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import ru.prolib.caelum.backnode.ValueFormatter;
import ru.prolib.caelum.lib.ITuple;

public class TupleMvcAdapter {
	private final ValueFormatter formatter;
	private final ITuple tuple;
	
	public TupleMvcAdapter(ITuple tuple, ValueFormatter formatter) {
		this.tuple = tuple;
		this.formatter = formatter;
	}
	
	public TupleMvcAdapter(ITuple tuple) {
		this(tuple, ValueFormatter.getInstance());
	}
	
	public ITuple getTuple() {
		return tuple;
	}
	
	@Override
	public int hashCode() {
		return new HashCodeBuilder(1451200203, 59)
				.append(tuple)
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
				.append(o.tuple, tuple)
				.append(o.formatter, formatter)
				.build();
	}
	
	public long getTime() {
		return tuple.getTime();
	}
	
	public String getTimeAsInstant() {
		return tuple.getTimeAsInstant().toString();
	}
	
	public String getOpen() {
		return formatter.format(tuple.getOpen(), tuple.getDecimals());
	}
	
	public String getHigh() {
		return formatter.format(tuple.getHigh(), tuple.getDecimals());
	}
	
	public String getLow() {
		return formatter.format(tuple.getLow(), tuple.getDecimals());
	}
	
	public String getClose() {
		return formatter.format(tuple.getClose(), tuple.getDecimals());
	}
	
	public String getVolume() {
		return formatter.format(tuple.getVolume(), tuple.getBigVolume(), tuple.getVolumeDecimals());
	}

}
