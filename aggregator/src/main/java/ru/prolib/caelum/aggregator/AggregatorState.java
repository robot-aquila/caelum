package ru.prolib.caelum.aggregator;

public enum AggregatorState {
	CREATED,
	PENDING,
	STARTING,
	RUNNING,
	STOPPING,
	ERROR,
	DEAD
}
