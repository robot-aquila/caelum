package ru.prolib.caelum.service.symboldb.fdb;

import java.io.IOException;

import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;

import ru.prolib.caelum.service.GeneralConfig;
import ru.prolib.caelum.service.IBuildingContext;
import ru.prolib.caelum.service.ICategoryExtractor;
import ru.prolib.caelum.service.symboldb.ISymbolService;
import ru.prolib.caelum.service.symboldb.ISymbolServiceBuilder;

public class FDBSymbolServiceBuilder implements ISymbolServiceBuilder {
	
	protected FDBSchema createSchema(String subspace) {
		return new FDBSchema(new Subspace(Tuple.from(subspace)));
	}
	
	protected ICategoryExtractor createCategoryExtractor(String class_name) throws IOException {
		try {
			return (ICategoryExtractor) Class.forName(class_name).getDeclaredConstructor().newInstance();
		} catch ( Exception e ) {
			throw new IOException("Category extractor instantiation failed", e);
		}
	}

	@Override
	public ISymbolService build(IBuildingContext context) throws IOException {
		GeneralConfig config = context.getConfig();
		FDBSymbolService service = new FDBSymbolService(
				createCategoryExtractor(config.getSymbolServiceCategoryExtractor()),
				createSchema(config.getFdbSubspace()), config.getMaxSymbolsLimit(), config.getMaxEventsLimit());
		context.registerService(new FDBDatabaseService(service, config.getFdbCluster()));
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
