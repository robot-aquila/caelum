package ru.prolib.caelum.aggregator;

import ru.prolib.caelum.core.AbstractConfig;

public class AggregatorConfig extends AbstractConfig {
	public static final String DEFAULT_CONFIG_FILE		= "app.aggregator.properties";
	public static final String BUILDER					= "caelum.aggregator.builder";
	public static final String INTERVAL					= "caelum.aggregator.interval";
	public static final String LIST_TUPLES_LIMIT		= "caelum.aggregator.list.tuples.limit";
	
	@Override
	public String getDefaultConfigFile() {
		return DEFAULT_CONFIG_FILE;
	}

	@Override
	protected void setDefaults() {
		props.put(BUILDER, "ru.prolib.caelum.aggregator.kafka.KafkaAggregatorServiceBuilder");
		props.put(INTERVAL, "M1,H1");
		props.put(LIST_TUPLES_LIMIT, "5000");
	}
	
	public int getListTuplesLimit() {
		return getInt(LIST_TUPLES_LIMIT);
	}

}
