package ru.prolib.caelum.core;

import java.math.BigInteger;

public interface ILBOHLCV extends IOHLCV {
	long getOpenPrice();
	long getHighPrice();
	long getLowPrice();
	long getClosePrice();
	long getVolume();
	BigInteger getBigVolume();
	byte getPriceDecimals();
	byte getVolumeDecimals();
}
