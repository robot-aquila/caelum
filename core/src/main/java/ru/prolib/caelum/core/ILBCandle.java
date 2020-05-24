package ru.prolib.caelum.core;

import java.math.BigInteger;

public interface ILBCandle extends ICandle {
	long getOpenPrice();
	long getHighPrice();
	long getLowPrice();
	long getClosePrice();
	long getVolume();
	BigInteger getBigVolume();
	byte getPriceDecimals();
	byte getVolumeDecimals();
}
