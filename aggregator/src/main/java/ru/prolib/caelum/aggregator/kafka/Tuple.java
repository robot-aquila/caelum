package ru.prolib.caelum.aggregator.kafka;

import java.math.BigInteger;
import java.time.Instant;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import ru.prolib.caelum.core.ITuple;
import ru.prolib.caelum.core.TupleType;

public class Tuple implements ITuple {
	private final String symbol;
	private final long time;
	private final KafkaTuple tuple;
	
	public Tuple(String symbol, long time, KafkaTuple tuple) {
		this.symbol = symbol;
		this.time = time;
		this.tuple = tuple;
	}

	@Override
	public String getSymbol() {
		return symbol;
	}

	@Override
	public long getTime() {
		return time;
	}

	@Override
	public Instant getTimeAsInstant() {
		return Instant.ofEpochMilli(time);
	}

	@Override
	public TupleType getType() {
		return tuple.getType();
	}

	@Override
	public long getOpen() {
		return tuple.getOpen();
	}

	@Override
	public long getHigh() {
		return tuple.getHigh();
	}

	@Override
	public long getLow() {
		return tuple.getLow();
	}

	@Override
	public long getClose() {
		return tuple.getClose();
	}

	@Override
	public long getVolume() {
		return tuple.getVolume();
	}

	@Override
	public BigInteger getBigVolume() {
		return tuple.getBigVolume();
	}

	@Override
	public byte getDecimals() {
		return tuple.getDecimals();
	}

	@Override
	public byte getVolumeDecimals() {
		return tuple.getVolumeDecimals();
	}
	
	@Override
	public int hashCode() {
		return new HashCodeBuilder(486929107, 95)
				.append(symbol)
				.append(time)
				.append(tuple)
				.build();
	}
	
	@Override
	public String toString() {
		return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
				.append("symbol", symbol)
				.append("time", time)
				.append("tuple", tuple)
				.build();
	}
	
	@Override
	public boolean equals(Object other) {
		if ( other == this ) {
			return true;
		}
		if ( other == null || other instanceof ITuple == false ) {
			return false;
		}
		ITuple o = (ITuple) other;
		return new EqualsBuilder()
				.append(o.getSymbol(), getSymbol())
				.append(o.getTime(), getTime())
				.append(o.getOpen(), getOpen())
				.append(o.getHigh(), getHigh())
				.append(o.getLow(), getLow())
				.append(o.getClose(), getClose())
				.append(o.getDecimals(), getDecimals())
				.append(o.getVolume(), getVolume())
				.append(o.getBigVolume(), getBigVolume())
				.append(o.getVolumeDecimals(), getVolumeDecimals())
				.append(o.getType(), getType())
				.build();
	}

}
