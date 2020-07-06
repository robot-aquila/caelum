package ru.prolib.caelum.symboldb;

import ru.prolib.caelum.core.AbstractConfig;
import ru.prolib.caelum.symboldb.fdb.FDBSymbolServiceBuilder;

/**
 * Base class for symbol database service configuration.
 * Used by top-level builder to determine exact components.
 */
public class SymbolServiceConfig extends AbstractConfig {
	public static final String DEFAULT_CONFIG_FILE = "app.symboldb.properties";
	public static final String BUILDER				= "caelum.symboldb.builder";
	public static final String CATEGORY_EXTRACTOR	= "caelum.symboldb.category.extractor";
	public static final String LIST_SYMBOLS_LIMIT	= "caelum.symboldb.list.symbols.limit";
	
	public SymbolServiceConfig() {
		super();
	}

	@Override
	protected void setDefaults() {
		props.put(BUILDER, FDBSymbolServiceBuilder.class.getName());
		props.put(CATEGORY_EXTRACTOR, CommonCategoryExtractor.class.getName());
		props.put(LIST_SYMBOLS_LIMIT, "5000");
	}
	
	@Override
	public String getDefaultConfigFile() {
		return DEFAULT_CONFIG_FILE;
	}
	
}