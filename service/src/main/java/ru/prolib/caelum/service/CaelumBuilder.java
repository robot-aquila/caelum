package ru.prolib.caelum.service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.prolib.caelum.aggregator.AggregatorServiceBuilder;
import ru.prolib.caelum.aggregator.IAggregatorServiceBuilder;
import ru.prolib.caelum.core.CompositeService;
import ru.prolib.caelum.itemdb.IItemDatabaseServiceBuilder;
import ru.prolib.caelum.itemdb.ItemDatabaseServiceBuilder;
import ru.prolib.caelum.symboldb.ISymbolServiceBuilder;
import ru.prolib.caelum.symboldb.SymbolServiceBuilder;

public class CaelumBuilder {
	private static final Logger logger = LoggerFactory.getLogger(CaelumBuilder.class);
	
	protected CaelumConfig createConfig() {
		return new CaelumConfig();
	}
	
	protected IItemDatabaseServiceBuilder createItemDatabaseServiceBuilder() {
		return new ItemDatabaseServiceBuilder();
	}
	
	protected IAggregatorServiceBuilder createAggregatorServiceBuilder() {
		return new AggregatorServiceBuilder();
	}
	
	protected ISymbolServiceBuilder createSymbolServiceBuilder() {
		return new SymbolServiceBuilder();
	}
	
	protected IExtensionBuilder createExtensionBuilder(String class_name) throws IOException {
		try {
			return (IExtensionBuilder) Class.forName(class_name).newInstance();
		} catch ( Exception e ) {
			throw new IOException("Exteension builder instantiation failed", e);
		}
	}
	
	public ICaelum build(String default_config_file, String config_file, CompositeService services)
		throws IOException
	{
		CaelumConfig config = createConfig();
		config.load(default_config_file, config_file);
		logger.debug("Configuration loaded: ");
		config.print(logger);
		List<IExtension> extensions = new ArrayList<>(); 
		ICaelum caelum = new Caelum(
				createAggregatorServiceBuilder().build(default_config_file, config_file, services),
				createItemDatabaseServiceBuilder().build(default_config_file, config_file, services),
				createSymbolServiceBuilder().build(default_config_file, config_file, services),
				extensions
			);
		for ( ExtensionConf c : config.getExtensions() ) {
			if ( c.isEnabled() ) {
				logger.debug("Loading extension: {}", c);
				extensions.add(createExtensionBuilder(c.getBuilderClass())
						.build(default_config_file, config_file, services, caelum));
			}
		}
		return caelum;
	}
	
}
