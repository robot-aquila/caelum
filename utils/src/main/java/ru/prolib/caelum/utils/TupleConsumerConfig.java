package ru.prolib.caelum.utils;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;

import ru.prolib.caelum.core.AbstractConfig;

public class TupleConsumerConfig extends AbstractConfig {
	public static final String DEFAULT_CONFIG_FILE	= "app.tupleconsumer.properties";
	public static final String BOOTSTRAP_SERVERS	= "caelum.tupleconsumer.bootstrap.servers";
	public static final String GROUP_ID				= "caelum.tupleconsumer.group.id";
	public static final String SOURCE_TOPIC			= "caelum.tupleconsumer.source.topic";
	
	@Override
	public String getDefaultConfigFile() {
		return DEFAULT_CONFIG_FILE;
	}
	
	@Override
	public void setDefaults() {
		props.put(BOOTSTRAP_SERVERS, "localhost:8082");
		props.put(GROUP_ID, "caelum-tuple-consumer");
		props.put(SOURCE_TOPIC, "caelum-tuple-m1");
	}
	
	public String getSourceTopic() {
		return getString(SOURCE_TOPIC);
	}

	public Properties getKafkaProperties() {
		Properties conf = new Properties();
		conf.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, props.get(BOOTSTRAP_SERVERS));
		conf.put(ConsumerConfig.GROUP_ID_CONFIG, props.get(GROUP_ID));
		conf.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		conf.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
		return conf;
	}
	
}
