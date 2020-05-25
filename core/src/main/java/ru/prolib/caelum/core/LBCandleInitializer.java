package ru.prolib.caelum.core;

import org.apache.kafka.streams.kstream.Initializer;

public class LBCandleInitializer implements Initializer<LBCandleMutable> {

	@Override
	public LBCandleMutable apply() {
		return new LBCandleMutable();
	}

}
