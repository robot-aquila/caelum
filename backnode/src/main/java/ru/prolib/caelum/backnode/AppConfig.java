package ru.prolib.caelum.backnode;

import java.io.IOException;

import ru.prolib.caelum.aggregator.kafka.KafkaAggregatorConfig;
import ru.prolib.caelum.itemdb.kafka.KafkaItemDatabaseConfig;

public class AppConfig {
	public static final String DEFAULT_CONFIG_FILE		= "app.backnode.properties";
	private final KafkaAggregatorConfig itemAggrConfig;
	private final KafkaItemDatabaseConfig itemDbConfig;
	
	AppConfig(KafkaAggregatorConfig item_aggr_config, KafkaItemDatabaseConfig itemdb_config) {
		this.itemAggrConfig = item_aggr_config;
		this.itemDbConfig = itemdb_config;
	}
	
	public AppConfig() {
		this(new KafkaAggregatorConfig(), new KafkaItemDatabaseConfig());
	}
	
	public KafkaAggregatorConfig getItemAggregatorConfig() {
		return itemAggrConfig;
	}
	
	public KafkaItemDatabaseConfig getItemDatabaseConfig() {
		return itemDbConfig;
	}
	
	public void load(String config_file) throws IOException {
		itemAggrConfig.load(DEFAULT_CONFIG_FILE, config_file);
		itemDbConfig.load(DEFAULT_CONFIG_FILE, config_file);
	}
	
	public void load() throws IOException {
		load(null);
	}

}
