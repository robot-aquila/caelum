package ru.prolib.caelum.lib;

import java.time.Instant;

public interface IItem {
	String getSymbol();
	long getTime();
	long getOffset();
	Instant getTimeAsInstant();
	ItemType getType();
	long getValue();
	long getVolume();
	byte getDecimals();
	byte getVolumeDecimals();
}
