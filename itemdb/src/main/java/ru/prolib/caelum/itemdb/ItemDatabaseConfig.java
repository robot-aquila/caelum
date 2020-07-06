package ru.prolib.caelum.itemdb;

import ru.prolib.caelum.core.AbstractConfig;
import ru.prolib.caelum.itemdb.kafka.KafkaItemDatabaseServiceBuilder;

public class ItemDatabaseConfig extends AbstractConfig {
	public static final String DEFAULT_CONFIG_FILE	= "app.itemdb.properties";
	public static final String BUILDER				= "caelum.itemdb.builder";
	public static final String LIST_ITEMS_LIMIT		= "caelum.itemdb.list.items.limit";

	@Override
	protected void setDefaults() {
		props.put(BUILDER, KafkaItemDatabaseServiceBuilder.class.getName());
		props.put(LIST_ITEMS_LIMIT, "5000");
	}

	@Override
	public String getDefaultConfigFile() {
		return DEFAULT_CONFIG_FILE;
	}

}
