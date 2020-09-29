package ru.prolib.caelum.service.itemdb;

import java.io.IOException;

import ru.prolib.caelum.service.IBuildingContext;

public class ItemDatabaseServiceBuilder implements IItemDatabaseServiceBuilder {

	protected ItemDatabaseConfig createConfig() {
		return new ItemDatabaseConfig();
	}
	
	protected IItemDatabaseServiceBuilder createBuilder(String class_name) throws IOException {
		try {
			return (IItemDatabaseServiceBuilder) Class.forName(class_name).newInstance();
		} catch ( Exception e ) {
			throw new IOException("ItemDB service builder instantiation failed", e);
		}
	}
	
	@Override
	public IItemDatabaseService build(IBuildingContext context) throws IOException {
		ItemDatabaseConfig config = createConfig();
		config.load(context.getDefaultConfigFileName(), context.getConfigFileName());
		IItemDatabaseServiceBuilder builder = createBuilder(config.getString(ItemDatabaseConfig.BUILDER));
		return builder.build(context);
	}
	
	@Override
	public int hashCode() {
		return 998102811;
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
