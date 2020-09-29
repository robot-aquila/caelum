package ru.prolib.caelum.service.symboldb;

import java.io.IOException;

import ru.prolib.caelum.service.IBuildingContext;

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
	public ISymbolService build(IBuildingContext context) throws IOException {
		SymbolServiceConfig config = createConfig();
		config.load(context.getDefaultConfigFileName(), context.getConfigFileName());
		ISymbolServiceBuilder builder = createBuilder(config.getString(SymbolServiceConfig.BUILDER));
		return builder.build(context);
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
