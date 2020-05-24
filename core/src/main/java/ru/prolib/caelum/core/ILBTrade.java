package ru.prolib.caelum.core;

public interface ILBTrade extends ITrade {
	long getPrice();
	long getVolume();
	byte getPriceDecimals();
	byte getVolumeDecimals();
}
