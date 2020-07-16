package ru.prolib.caelum.itemdb.kafka;

import java.io.IOException;

import org.apache.kafka.clients.producer.KafkaProducer;

import ru.prolib.caelum.core.CompositeService;
import ru.prolib.caelum.itemdb.IItemDatabaseService;
import ru.prolib.caelum.itemdb.IItemDatabaseServiceBuilder;

public class KafkaItemDatabaseServiceBuilder implements IItemDatabaseServiceBuilder {
	private final KafkaUtils utils;
	
	public KafkaItemDatabaseServiceBuilder(KafkaUtils utils) {
		this.utils = utils;
	}
	
	public KafkaItemDatabaseServiceBuilder() {
		this(KafkaUtils.getInstance());
	}
	
	public KafkaUtils getUtils() {
		return utils;
	}
	
	protected KafkaItemDatabaseConfig createConfig() {
		return new KafkaItemDatabaseConfig();
	}
	
	protected KafkaItemDatabaseService createService(KafkaItemDatabaseConfig config,
			KafkaProducer<String, KafkaItem> producer)
	{
		return new KafkaItemDatabaseService(config, producer, utils);
	}

	@Override
	public IItemDatabaseService build(String default_config_file, String config_file, CompositeService services)
			throws IOException
	{
		KafkaItemDatabaseConfig config = createConfig();
		config.load(default_config_file, config_file);
		KafkaProducer<String, KafkaItem> producer = utils.createProducer(config.getProducerKafkaProperties());
		services.register(new KafkaProducerService(producer));
		return createService(config, producer);
	}
	
	@Override
	public int hashCode() {
		return 50098172;
	}
	
	@Override
	public boolean equals(Object other) {
		if ( other == this ) {
			return true;
		}
		if ( other == null || other.getClass() != KafkaItemDatabaseServiceBuilder.class ) {
			return false;
		}
		return true;
	}

}
