package ru.prolib.caelum.itemdb;

import java.io.IOException;

import ru.prolib.caelum.core.CompositeService;

public interface IItemDatabaseServiceBuilder {
	/**
	 * General itemdb service builder.
	 * <p>
	 * @param config_file - path to config file if specified (can be null).
	 * @param services - services can be used if one or more services should be registered
	 * @return item database service instance
	 * @throws IOException - an error occurred
	 */
	IItemDatabaseService build(String config_file, CompositeService services) throws IOException;
}
