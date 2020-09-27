package ru.prolib.caelum.service.aggregator;

public enum AggregatorState {
	CREATED,
	PENDING,
	STARTING,
	RUNNING,
	STOPPING,
	ERROR,
	DEAD
}
