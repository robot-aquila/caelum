package ru.prolib.caelum.service.aggregator;

import java.io.IOException;

import ru.prolib.caelum.lib.CompositeService;

public interface IAggregatorServiceBuilder {
	/**
	 * General aggregator service builder.
	 * <p>
	 * @param default_config_file - path to default config file
	 * @param config_file - path to config file if specified (can be null).
	 * @param services - services can be used if one or more services should be registered
	 * @return aggregator service instance
	 * @throws IOException - an error occurred
	 */
	IAggregatorService build(String default_config_file, String config_file, CompositeService services)
			throws IOException;
}
