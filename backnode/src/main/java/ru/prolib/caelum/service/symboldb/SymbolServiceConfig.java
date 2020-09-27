package ru.prolib.caelum.service.symboldb;

import ru.prolib.caelum.lib.AbstractConfig;
import ru.prolib.caelum.service.symboldb.fdb.FDBSymbolServiceBuilder;

/**
 * Base class for symbol database service configuration.
 * Used by top-level builder to determine exact components.
 */
public class SymbolServiceConfig extends AbstractConfig {
	public static final String DEFAULT_CONFIG_FILE = "app.symboldb.properties";
	public static final String BUILDER				= "caelum.symboldb.builder";
	public static final String CATEGORY_EXTRACTOR	= "caelum.symboldb.category.extractor";
	public static final String LIST_SYMBOLS_LIMIT	= "caelum.symboldb.list.symbols.limit";
	public static final String LIST_EVENTS_LIMIT	= "caelum.symboldb.list.events.limit";
	
	public SymbolServiceConfig() {
		super();
	}

	@Override
	protected void setDefaults() {
		props.put(BUILDER, FDBSymbolServiceBuilder.class.getName());
		props.put(CATEGORY_EXTRACTOR, CommonCategoryExtractor.class.getName());
		props.put(LIST_SYMBOLS_LIMIT, "5000");
		props.put(LIST_EVENTS_LIMIT, "5000");
	}
	
	@Override
	public String getDefaultConfigFile() {
		return DEFAULT_CONFIG_FILE;
	}
	
}
