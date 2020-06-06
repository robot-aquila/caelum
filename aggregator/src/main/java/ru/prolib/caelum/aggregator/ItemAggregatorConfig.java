package ru.prolib.caelum.aggregator;

import java.io.IOException;
import java.time.Duration;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;

import ru.prolib.caelum.core.AbstractConfig;
import ru.prolib.caelum.core.Periods;
import ru.prolib.caelum.core.CaelumSerdes;

public class ItemAggregatorConfig extends AbstractConfig {
	public static final String DEFAULT_CONFIG_FILE		= "app.itemaggregator.properties";
	public static final String APPLICATION_ID_PREFIX	= "caelum.itemaggregator.pfx.application.id";
	public static final String AGGREGATION_STORE_PREFIX	= "caelum.itemaggregator.pfx.aggregation.store";
	public static final String TARGET_TOPIC_PREFIX		= "caelum.itemaggregator.pfx.target.topic";
	public static final String BOOTSTRAP_SERVERS		= "caelum.itemaggregator.bootstrap.servers";
	public static final String SOURCE_TOPIC				= "caelum.itemaggregator.source.topic";
	public static final String AGGREGATION_PERIOD		= "caelum.itemaggregator.aggregation.period";
	
	@Override
	public void setDefaults() {
		props.put(APPLICATION_ID_PREFIX, "caelum-item-aggregator-");
		props.put(AGGREGATION_STORE_PREFIX, "caelum-tuple-store-");
		props.put(TARGET_TOPIC_PREFIX, "caelum-tuple-");
		props.put(BOOTSTRAP_SERVERS, "localhost:8082");
		props.put(SOURCE_TOPIC, "caelum-item");
		props.put(AGGREGATION_PERIOD, "M1");
	}
	
	private String getSuffix() {
		return getString(AGGREGATION_PERIOD).toLowerCase();
	}
	
	public void loadFromResources() throws IOException {
		loadFromResources(DEFAULT_CONFIG_FILE);
	}

	public String getApplicationId() {
		return getString(APPLICATION_ID_PREFIX) + getSuffix();
	}

	public String getStoreName() {
		return getString(AGGREGATION_STORE_PREFIX) + getSuffix();
	}
	
	public Duration getAggregationPeriod() {
		return Periods.getInstance().getIntradayDurationByCode(getString(AGGREGATION_PERIOD));
	}
	
	public String getTargetTopic() {
		return getString(TARGET_TOPIC_PREFIX) + getSuffix();
	}
	
	@Override
	public Properties getKafkaProperties() {
		Properties conf = new Properties();
		conf.put(StreamsConfig.APPLICATION_ID_CONFIG, getApplicationId());
		conf.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, props.get(BOOTSTRAP_SERVERS));
		conf.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		conf.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, CaelumSerdes.itemSerde().getClass());
		return conf;
	}

}
