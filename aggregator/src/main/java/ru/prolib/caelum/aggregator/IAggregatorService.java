package ru.prolib.caelum.aggregator;

import ru.prolib.caelum.core.ICloseableIterator;
import ru.prolib.caelum.core.ITuple;

public interface IAggregatorService {
	ICloseableIterator<ITuple> fetch(AggregatedDataRequest request);
}
