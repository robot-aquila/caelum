package ru.prolib.caelum.aggregator;

public enum AggregatorType {
	/**
	 * From item to tuple aggregation with persistent data storage.
	 */
	ITEM,
	/**
	 * Aggregation from tuples of smaller period to tuple of bigger with persistent data storage.
	 */
	TUPLE,
	/**
	 * Aggregation from tuple of smaller period to tuple of bigger without storing the data.
	 */
	TUPLE_ONFLY
}
