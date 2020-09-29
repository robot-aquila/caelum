package ru.prolib.caelum.service.itemdb;

import java.io.IOException;

import ru.prolib.caelum.service.IBuildingContext;

public interface IItemDatabaseServiceBuilder {
	/**
	 * General itemdb service builder.
	 * <p>
	 * @param context - building context
	 * @return item database service instance
	 * @throws IOException - an error occurred
	 */
	IItemDatabaseService build(IBuildingContext context) throws IOException;
}
