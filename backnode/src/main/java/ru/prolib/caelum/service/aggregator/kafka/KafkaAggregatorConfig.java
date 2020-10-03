package ru.prolib.caelum.service.aggregator.kafka;

import java.util.Properties;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.streams.StreamsConfig;

import ru.prolib.caelum.lib.HostInfo;
import ru.prolib.caelum.lib.Interval;
import ru.prolib.caelum.lib.kafka.KafkaItemSerdes;
import ru.prolib.caelum.service.GeneralConfig;

public class KafkaAggregatorConfig {
	private final Interval interval;
	private final GeneralConfig config;
	
	public KafkaAggregatorConfig(Interval interval, GeneralConfig config) {
		super();
		this.interval = interval;
		this.config = config;
	}
	
	public Interval getInterval() {
		return interval;
	}
	
	public GeneralConfig getConfig() {
		return config;
	}
	
	private String getSuffix() {
		return interval.getCode().toLowerCase();
	}

	public String getApplicationId() {
		return config.getAggregatorKafkaApplicationIdPrefix() + getSuffix();
	}

	public String getStoreName() {
		return config.getAggregatorKafkaStorePrefix() + getSuffix();
	}
	
	public long getStoreRetentionTime() {
		return config.getAggregatorKafkaStoreRetentionTime();
	}
	
	public Interval getAggregationInterval() {
		return interval;
	}
	
	/**
	 * Get topic to store aggregated data.
	 * <p>
	 * @return topic or null if topic is not defined and data shouldn't be stored
	 */
	public String getTargetTopic() {
		String prefix = config.getAggregatorKafkaTargetTopicPrefix();
		return "".equals(prefix) ? null : prefix + getSuffix();
	}
	
	public String getSourceTopic() {
		return config.getItemsTopicName();
	}
	
	public int getMaxErrors() {
		return config.getMaxErrors();
	}
	
	public long getDefaultTimeout() {
		return config.getDefaultTimeout();
	}
	
	// Not used inside aggregator
	@Deprecated
	public HostInfo getApplicationServer() {
		return config.getHttpInfo();
	}
	
	public Properties getKafkaStreamsProperties() {
		Properties conf = new Properties();
		conf.put(StreamsConfig.APPLICATION_ID_CONFIG, getApplicationId());
		conf.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, config.getKafkaBootstrapServers());
		conf.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, KafkaItemSerdes.keySerde().getClass());
		conf.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, KafkaItemSerdes.itemSerde().getClass());
		conf.put(StreamsConfig.STATE_DIR_CONFIG, config.getKafkaStateDir());
		conf.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, Integer.toString(config.getAggregatorKafkaNumStreamThreads()));
		conf.put(StreamsConfig.APPLICATION_SERVER_CONFIG, config.getHttpInfo().toString());
		conf.put(ProducerConfig.LINGER_MS_CONFIG, Long.toString(config.getAggregatorKafkaLingerMs()));
		
		conf.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
		//conf.put(StreamsConfig.ACCEPTABLE_RECOVERY_LAG_CONFIG, "500");
		//conf.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "500");
		//conf.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, "60000");
		return conf;
	}
	
	@Override
	public int hashCode() {
		return new HashCodeBuilder(752913, 47)
				.append(interval)
				.append(config)
				.build();
	}
	
	@Override
	public boolean equals(Object other) {
		if ( other == this ) {
			return true;
		}
		if ( other == null || other.getClass() != KafkaAggregatorConfig.class ) {
			return false;
		}
		KafkaAggregatorConfig o = (KafkaAggregatorConfig) other;
		return new EqualsBuilder()
				.append(o.interval, interval)
				.append(o.config, config)
				.build();
	}

}
