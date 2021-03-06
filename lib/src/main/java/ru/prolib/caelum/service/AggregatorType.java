package ru.prolib.caelum.service;

public enum AggregatorType {
	/**
	 * From item to tuple aggregation with persistent data storage.
	 */
	ITEM,
	/**
	 * Aggregation from tuples of lesser interval to tuple of bigger with persistent data storage.
	 */
	TUPLE,
	/**
	 * Aggregation from tuple of lesser interval to tuple of bigger without storing the data.
	 */
	TUPLE_ONFLY
}
