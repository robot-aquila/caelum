package ru.prolib.caelum.itemdb.kafka;

import java.io.IOException;

import ru.prolib.caelum.core.CompositeService;
import ru.prolib.caelum.itemdb.IItemDatabaseService;
import ru.prolib.caelum.itemdb.IItemDatabaseServiceBuilder;

public class ItemDatabaseServiceBuilder implements IItemDatabaseServiceBuilder {
	
	protected ItemDatabaseConfig createConfig() {
		return new ItemDatabaseConfig();
	}
	
	protected ItemDatabaseService createService(ItemDatabaseConfig config) {
		return new ItemDatabaseService(config);
	}

	@Override
	public IItemDatabaseService build(String config_file, CompositeService services) throws IOException {
		ItemDatabaseConfig config = createConfig();
		config.load(config_file);
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
		if ( other == null || other.getClass() != ItemDatabaseServiceBuilder.class ) {
			return false;
		}
		return true;
	}

}
