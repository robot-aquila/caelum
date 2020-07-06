package ru.prolib.caelum.itemdb.kafka;

import java.io.IOException;

import ru.prolib.caelum.core.CompositeService;
import ru.prolib.caelum.itemdb.IItemDatabaseService;
import ru.prolib.caelum.itemdb.IItemDatabaseServiceBuilder;

public class KafkaItemDatabaseServiceBuilder implements IItemDatabaseServiceBuilder {
	
	protected KafkaItemDatabaseConfig createConfig() {
		return new KafkaItemDatabaseConfig();
	}
	
	protected KafkaItemDatabaseService createService(KafkaItemDatabaseConfig config) {
		return new KafkaItemDatabaseService(config);
	}

	@Override
	public IItemDatabaseService build(String default_config_file, String config_file, CompositeService services)
			throws IOException
	{
		KafkaItemDatabaseConfig config = createConfig();
		config.load(default_config_file, config_file);
		return createService(config);
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
