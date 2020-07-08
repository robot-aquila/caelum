package ru.prolib.caelum.itemdb.kafka;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;

import ru.prolib.caelum.itemdb.ItemDatabaseConfig;

public class KafkaItemDatabaseConfig extends ItemDatabaseConfig {
	public static final String BOOTSTRAP_SERVERS		= "caelum.itemdb.kafka.bootstrap.servers";
	public static final String GROUP_ID					= "caelum.itemdb.kafka.group.id";
	public static final String SOURCE_TOPIC				= "caelum.itemdb.kafka.source.topic";

	@Override
	protected void setDefaults() {
		super.setDefaults();
		props.put(BOOTSTRAP_SERVERS, "localhost:8082");
		props.put(GROUP_ID, "caelum-item-db");
		props.put(SOURCE_TOPIC, "caelum-item");
	}

	public Properties getKafkaProperties() {
		Properties conf = new Properties();
		conf.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, props.get(BOOTSTRAP_SERVERS));
		conf.put(ConsumerConfig.GROUP_ID_CONFIG, props.get(GROUP_ID));
		conf.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		conf.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
		return conf;
	}
	
	public String getSourceTopic() {
		return getString(SOURCE_TOPIC);
	}

}