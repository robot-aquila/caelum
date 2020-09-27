package ru.prolib.caelum.service.aggregator;

/**
 * Controller of a single aggregator instance.
 */
public interface IAggregator {
	AggregatorStatus getStatus();
	void clear(boolean global);
}
