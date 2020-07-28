package ru.prolib.caelum.aggregator.kafka;

import java.time.Duration;
import java.util.Properties;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.streams.StreamsConfig;

import ru.prolib.caelum.aggregator.AggregatorConfig;
import ru.prolib.caelum.core.Period;
import ru.prolib.caelum.core.Periods;
import ru.prolib.caelum.itemdb.kafka.KafkaItemSerdes;

public class KafkaAggregatorConfig extends AggregatorConfig {
	public static final String APPLICATION_ID_PREFIX	= "caelum.aggregator.kafka.pfx.application.id";
	public static final String AGGREGATION_STORE_PREFIX	= "caelum.aggregator.kafka.pfx.aggregation.store";
	public static final String TARGET_TOPIC_PREFIX		= "caelum.aggregator.kafka.pfx.target.topic";
	public static final String BOOTSTRAP_SERVERS		= "caelum.aggregator.kafka.bootstrap.servers";
	public static final String SOURCE_TOPIC				= "caelum.aggregator.kafka.source.topic";
	public static final String MAX_ERRORS				= "caelum.aggregator.kafka.max.errors";
	public static final String DEFAULT_TIMEOUT			= "caelum.aggregator.kafka.default.timeout";

	private final Periods periods;
	
	public KafkaAggregatorConfig(Periods periods) {
		super();
		this.periods = periods;
	}
	
	public Periods getPeriods() {
		return periods;
	}
	
	@Override
	public void setDefaults() {
		super.setDefaults();
		props.put(APPLICATION_ID_PREFIX, "caelum-item-aggregator-");
		props.put(AGGREGATION_STORE_PREFIX, "caelum-tuple-store-");
		props.put(TARGET_TOPIC_PREFIX, "caelum-tuple-");
		props.put(BOOTSTRAP_SERVERS, "localhost:8082");
		props.put(SOURCE_TOPIC, "caelum-item");
		props.put(MAX_ERRORS, "99");
		props.put(DEFAULT_TIMEOUT, "15000");
	}
	
	private String getSuffix() {
		return getString(AGGREGATION_PERIOD).toLowerCase();
	}
	

	public String getApplicationId() {
		return getString(APPLICATION_ID_PREFIX) + getSuffix();
	}

	public String getStoreName() {
		return getString(AGGREGATION_STORE_PREFIX) + getSuffix();
	}
	
	public String getAggregationPeriodCode() {
		return getOneOfList(KafkaAggregatorConfig.AGGREGATION_PERIOD, periods.getIntradayPeriodCodes());
	}
	
	public Period getAggregationPeriod() {
		return Period.valueOf(getAggregationPeriodCode());
	}
	
	public Duration getAggregationPeriodDuration() {
		return periods.getIntradayDurationByCode(getAggregationPeriodCode());
	}
	
	/**
	 * Get topic to store aggregated data.
	 * <p>
	 * @return topic or null if topic is not defined and data shouldn't be stored
	 */
	public String getTargetTopic() {
		String prefix = getString(TARGET_TOPIC_PREFIX);
		return "".equals(prefix) ? null : prefix + getSuffix();
	}
	
	public String getSourceTopic() {
		return getString(SOURCE_TOPIC);
	}
	
	public int getMaxErrors() {
		return getInt(MAX_ERRORS);
	}
	
	public long getDefaultTimeout() {
		return getInt(DEFAULT_TIMEOUT);
	}
	
	public Properties getKafkaProperties() {
		Properties conf = new Properties();
		conf.put(StreamsConfig.APPLICATION_ID_CONFIG, getApplicationId());
		conf.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, props.get(BOOTSTRAP_SERVERS));
		conf.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, KafkaItemSerdes.keySerde().getClass());
		conf.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, KafkaItemSerdes.itemSerde().getClass());
		return conf;
	}
	
	public Properties getAdminClientProperties() {
		Properties conf = new Properties();
		conf.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, props.get(BOOTSTRAP_SERVERS));
		return conf;
	}

}
