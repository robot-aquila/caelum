package ru.prolib.caelum.itesym.ak;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;

import ru.prolib.caelum.core.AbstractConfig;

public class ItesymConfig extends AbstractConfig {
	public static final String DEFAULT_CONFIG_FILE = "app.itesym.properties";
	public static final String BOOTSTRAP_SERVERS	= "caelum.itesym.bootstrap.servers";
	public static final String GROUP_ID				= "caelum.itesym.group.id";
	public static final String SOURCE_TOPIC			= "caelum.itesym.source.topic";
	public static final String POLL_TIMEOUT			= "caelum.itesym.poll.timeout";
	public static final String SHUTDOWN_TIMEOUT		= "caelum.itesym.shutdown.timeout";

	@Override
	protected String getDefaultConfigFile() {
		return DEFAULT_CONFIG_FILE;
	}
	
	@Override
	protected void setDefaults() {
		props.put(BOOTSTRAP_SERVERS, "localhost:8082");
		props.put(GROUP_ID, "caelum-itesym");
		props.put(SOURCE_TOPIC, "caelum-item");
		props.put(POLL_TIMEOUT, "1000");
		props.put(SHUTDOWN_TIMEOUT, "15000");
	}
	
	public String getSourceTopic() {
		return getString(SOURCE_TOPIC);
	}
	
	public long getPollTimeout() {
		return getInt(POLL_TIMEOUT);
	}
	
	public long getShutdownTimeout() {
		return getInt(SHUTDOWN_TIMEOUT);
	}
	
	public String getGroupId() {
		return getString(GROUP_ID);
	}
	
	public Properties getKafkaProperties() {
		Properties conf = new Properties();
		conf.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, getString(BOOTSTRAP_SERVERS));
		conf.put(ConsumerConfig.GROUP_ID_CONFIG, getString(GROUP_ID));
		conf.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
		conf.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
		return conf;
	}

}
