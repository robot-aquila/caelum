package ru.prolib.caelum.service.aggregator;

import java.io.IOException;

import ru.prolib.caelum.service.IBuildingContext;

public interface IAggregatorServiceBuilder {
	/**
	 * General aggregator service builder.
	 * <p>
	 * @param context - building context
	 * @return aggregator service instance
	 * @throws IOException - an error occurred
	 */
	IAggregatorService build(IBuildingContext context) throws IOException;
}
