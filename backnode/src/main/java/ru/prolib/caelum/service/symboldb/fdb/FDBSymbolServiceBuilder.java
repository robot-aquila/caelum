package ru.prolib.caelum.service.symboldb.fdb;

import java.io.IOException;

import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;

import ru.prolib.caelum.service.IBuildingContext;
import ru.prolib.caelum.service.ICategoryExtractor;
import ru.prolib.caelum.service.symboldb.ISymbolService;
import ru.prolib.caelum.service.symboldb.ISymbolServiceBuilder;

public class FDBSymbolServiceBuilder implements ISymbolServiceBuilder {

	protected FDBSymbolServiceConfig createConfig() {
		return new FDBSymbolServiceConfig();
	}
	
	protected FDBSchema createSchema(String subspace) {
		return new FDBSchema(new Subspace(Tuple.from(subspace)));
	}
	
	protected ICategoryExtractor createCategoryExtractor(String class_name) throws IOException {
		try {
			return (ICategoryExtractor) Class.forName(class_name).newInstance();
		} catch ( Exception e ) {
			throw new IOException("Category extractor instantiation failed", e);
		}
	}

	@Override
	public ISymbolService build(IBuildingContext context) throws IOException {
		FDBSymbolServiceConfig config = createConfig();
		config.load(context.getDefaultConfigFileName(), context.getConfigFileName());
		FDBSymbolService service = new FDBSymbolService(
				createCategoryExtractor(config.getString(FDBSymbolServiceConfig.CATEGORY_EXTRACTOR)),
				createSchema(config.getString(FDBSymbolServiceConfig.SUBSPACE)),
				config.getInt(FDBSymbolServiceConfig.LIST_SYMBOLS_LIMIT),
				config.getInt(FDBSymbolServiceConfig.LIST_EVENTS_LIMIT));
		context.registerService(new FDBDatabaseService(service, config.getString(FDBSymbolServiceConfig.FDB_CLUSTER)));
		return service;
	}
	
	@Override
	public int hashCode() {
		return 2011572;
	}
	
	@Override
	public boolean equals(Object other) {
		if ( other == this ) {
			return true;
		}
		if ( other == null || other.getClass() != FDBSymbolServiceBuilder.class ) {
			return false;
		}
		return true;
	}

}
