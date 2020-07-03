package ru.prolib.caelum.aggregator;

import ru.prolib.caelum.core.Period;

public interface IAggregatorDescr {
	Period getPeriod();
	AggregatorType getType();
}
