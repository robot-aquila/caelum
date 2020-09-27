package ru.prolib.caelum.service.itemdb.kafka;

import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import ru.prolib.caelum.service.itemdb.ItemDatabaseConfig;

public class KafkaItemDatabaseConfig extends ItemDatabaseConfig {
	public static final String BOOTSTRAP_SERVERS		= "caelum.itemdb.kafka.bootstrap.servers";
	public static final String SOURCE_TOPIC				= "caelum.itemdb.kafka.source.topic";
	public static final String TRANSACTIONAL_ID			= "caelum.itemdb.kafka.transactional.id";

	@Override
	protected void setDefaults() {
		super.setDefaults();
		props.put(BOOTSTRAP_SERVERS, "localhost:8082");
		props.put(SOURCE_TOPIC, "caelum-item");
		props.put(TRANSACTIONAL_ID, "");
	}
	
	protected String randomUUID() {
		return UUID.randomUUID().toString();
	}
	
	protected String getTransactionalId() {
		String trans_id = props.getProperty(TRANSACTIONAL_ID);
		return trans_id == null || "".equals(trans_id) ? randomUUID() : trans_id;
	}
	
	public Properties getConsumerKafkaProperties() {
		Properties conf = new Properties();
		conf.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, props.get(BOOTSTRAP_SERVERS));
		conf.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		conf.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
		conf.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
		return conf;
	}
	
	public Properties getProducerKafkaProperties() {
		Properties conf = new Properties();
		conf.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, props.get(BOOTSTRAP_SERVERS));
		conf.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, getTransactionalId());
		return conf;
	}
	
	public Properties getAdminClientProperties() {
		Properties conf = new Properties();
		conf.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, props.get(BOOTSTRAP_SERVERS));
		return conf;
	}
	
	public String getSourceTopic() {
		return getString(SOURCE_TOPIC);
	}

}
