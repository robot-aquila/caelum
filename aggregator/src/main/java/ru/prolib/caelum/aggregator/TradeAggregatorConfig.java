package ru.prolib.caelum.aggregator;

import java.io.IOException;
import java.time.Duration;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;

import ru.prolib.caelum.core.AbstractConfig;
import ru.prolib.caelum.core.Periods;
import ru.prolib.caelum.core.CaelumSerdes;

public class TradeAggregatorConfig extends AbstractConfig {
	public static final String DEFAULT_CONFIG_FILE		= "app.tradeaggregator.properties";
	public static final String APPLICATION_ID_PREFIX	= "caelum.tradeaggregator.pfx.application.id";
	public static final String AGGREGATION_STORE_PREFIX	= "caelum.tradeaggregator.pfx.aggregation.store";
	public static final String TARGET_TOPIC_PREFIX		= "caelum.tradeaggregator.pfx.target.topic";
	public static final String BOOTSTRAP_SERVERS		= "caelum.tradeaggregator.bootstrap.servers";
	public static final String SOURCE_TOPIC				= "caelum.tradeaggregator.source.topic";
	public static final String AGGREGATION_PERIOD		= "caelum.tradeaggregator.aggregation.period";
	
	@Override
	public void setDefaults() {
		props.put(APPLICATION_ID_PREFIX, "caelum-trades-aggregator-");
		props.put(AGGREGATION_STORE_PREFIX, "caelum-ohlcv-store-");
		props.put(TARGET_TOPIC_PREFIX, "caelum-ohlcv-");
		props.put(BOOTSTRAP_SERVERS, "localhost:8082");
		props.put(SOURCE_TOPIC, "caelum-trades");
		props.put(AGGREGATION_PERIOD, "M1");
	}
	
	public void loadFromResources() throws IOException {
		loadFromResources(DEFAULT_CONFIG_FILE);
	}

	public String getApplicationId() {
		return getString(APPLICATION_ID_PREFIX) + getString(AGGREGATION_PERIOD).toLowerCase();
	}

	public String getStoreName() {
		return getString(AGGREGATION_STORE_PREFIX) + getString(AGGREGATION_PERIOD).toLowerCase();
	}
	
	public Duration getAggregationPeriod() {
		return Periods.getInstance().getIntradayDurationByCode(getString(AGGREGATION_PERIOD));
	}
	
	public String getTargetTopic() {
		return getString(TARGET_TOPIC_PREFIX) + getString(AGGREGATION_PERIOD).toLowerCase();
	}
	
	@Override
	public Properties getKafkaProperties() {
		Properties conf = new Properties();
		conf.put(StreamsConfig.APPLICATION_ID_CONFIG, getApplicationId());
		conf.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, props.get(BOOTSTRAP_SERVERS));
		conf.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		conf.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, CaelumSerdes.ILBTrade().getClass());
		return conf;
	}

}
