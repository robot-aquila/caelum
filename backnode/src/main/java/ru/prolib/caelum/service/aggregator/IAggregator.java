package ru.prolib.caelum.service.aggregator;

import ru.prolib.caelum.service.AggregatorStatus;

/**
 * Controller of a single aggregator instance.
 */
public interface IAggregator {
	AggregatorStatus getStatus();
	void clear(boolean global);
}
