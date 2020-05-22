package ru.prolib.caelum.core;

public enum TradeRecordType {
	RESERVED_0,
	/**
	 * This is base type for all long type based trades means that exact type is not yet determined.
	 */
	LONG_UNKNOWN,
	LONG_COMPACT,
	LONG_REGULAR,
	RESERVED_1
}
