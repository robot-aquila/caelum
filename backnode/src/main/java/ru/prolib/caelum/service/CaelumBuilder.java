package ru.prolib.caelum.service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.prolib.caelum.lib.Intervals;
import ru.prolib.caelum.service.aggregator.AggregatorServiceBuilder;
import ru.prolib.caelum.service.aggregator.IAggregatorServiceBuilder;
import ru.prolib.caelum.service.itemdb.IItemDatabaseServiceBuilder;
import ru.prolib.caelum.service.itemdb.ItemDatabaseServiceBuilder;
import ru.prolib.caelum.service.symboldb.ISymbolServiceBuilder;
import ru.prolib.caelum.service.symboldb.SymbolServiceBuilder;

public class CaelumBuilder {
	private static final Logger logger = LoggerFactory.getLogger(CaelumBuilder.class);
	
	protected GeneralConfigImpl createConfig() {
		return new GeneralConfigImpl(new Intervals());
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
	
	public ICaelum build(BuildingContext context) throws IOException {
		GeneralConfigImpl config = createConfig();
		config.load(context.getConfigFileName());
		logger.debug("Configuration loaded: ");
		config.print(logger);
		context = context.withConfig(config);
		ICaelum caelum;
		List<IExtension> extensions = new ArrayList<>();
		context = context.withCaelum(caelum = new Caelum(
				createAggregatorServiceBuilder().build(context),
				createItemDatabaseServiceBuilder().build(context),
				createSymbolServiceBuilder().build(context),
				extensions
			));
		for ( ExtensionConf c : config.getExtensions() ) {
			if ( c.isEnabled() ) {
				logger.debug("Loading extension: {}", c);
				extensions.add(createExtensionBuilder(c.getBuilderClass()).build(context));
			}
		}
		return caelum;
	}
	
}
