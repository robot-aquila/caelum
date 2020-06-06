package ru.prolib.caelum.test;

import java.util.Properties;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;

import ru.prolib.caelum.core.AbstractConfig;
import ru.prolib.caelum.core.CaelumSerdes;

public class TradeGeneratorConfig extends AbstractConfig {
	public static final String DEFAULT_CONFIG_FILE	= "app.tradegenerator.properties";
	public static final String BOOTSTRAP_SERVERS	= "caelum.tradegenerator.bootstrap.servers";
	public static final String TARGET_TOPIC			= "caelum.tradegenerator.target.topic";
	public static final String SEED					= "caelum.tradegenerator.seed";
	public static final String SYMBOL_NUM			= "caelum.tradegenerator.symbol.num";
	public static final String SYMBOL_CHARS			= "caelum.tradegenerator.symbol.chars";
	public static final String SYMBOL_PREFIX		= "caelum.tradegenerator.symbol.prefix";
	public static final String SYMBOL_SUFFIX		= "caelum.tradegenerator.symbol.suffix";
	public static final String TRADES_PER_MINUTE	= "caelum.tradegenerator.trades.per.minute";
	
	@Override
	public void setDefaults() {
		props.put(BOOTSTRAP_SERVERS, "localhost:8082");
		props.put(TARGET_TOPIC, "caelum-item");
		props.put(SEED, "459811");
		props.put(SYMBOL_NUM, "16");
		props.put(SYMBOL_CHARS, "4");
		props.put(SYMBOL_PREFIX, "S:");
		props.put(SYMBOL_SUFFIX, "@EXCHANGE:USD");
		props.put(TRADES_PER_MINUTE, "120");
	}

	@Override
	public Properties getKafkaProperties() {
		Properties conf = new Properties();
		conf.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, props.get(BOOTSTRAP_SERVERS));
		conf.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Serdes.String().serializer().getClass());
		conf.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CaelumSerdes.itemSerde().serializer().getClass());
		return conf;
	}
	
}
