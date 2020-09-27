package ru.prolib.caelum.lib;

import java.math.BigInteger;
import java.time.Instant;

public interface ITuple {
	String getSymbol();
	long getTime();
	Instant getTimeAsInstant();
	TupleType getType();
	long getOpen();
	long getHigh();
	long getLow();
	long getClose();
	long getVolume();
	BigInteger getBigVolume();
	byte getDecimals();
	byte getVolumeDecimals();
}
