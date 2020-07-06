package ru.prolib.caelum.symboldb;

import java.io.IOException;

import ru.prolib.caelum.core.CompositeService;

public class SymbolServiceBuilder implements ISymbolServiceBuilder {
	
	protected SymbolServiceConfig createConfig() {
		return new SymbolServiceConfig();
	}
	
	protected ISymbolServiceBuilder createBuilder(String class_name) throws IOException {
		try {
			return (ISymbolServiceBuilder) Class.forName(class_name).newInstance();
		} catch ( Exception e ) {
			throw new IOException("Symbol service builder instantiation failed", e);
		}
	}

	@Override
	public ISymbolService build(String default_config_file, String config_file, CompositeService services)
			throws IOException
	{
		SymbolServiceConfig config = createConfig();
		config.load(default_config_file, config_file);
		ISymbolServiceBuilder builder = createBuilder(config.getString(SymbolServiceConfig.BUILDER));
		return builder.build(default_config_file, config_file, services);
	}
	
	@Override
	public int hashCode() {
		return 5578912;
	}
	
	@Override
	public boolean equals(Object other) {
		if ( other == this ) {
			return true;
		}
		if ( other == null || other.getClass() != SymbolServiceBuilder.class ) {
			return false;
		}
		return true;
	}

}
