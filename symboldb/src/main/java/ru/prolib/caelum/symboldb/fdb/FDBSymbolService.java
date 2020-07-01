package ru.prolib.caelum.symboldb.fdb;

import com.apple.foundationdb.Database;

import ru.prolib.caelum.core.ICloseableIterator;
import ru.prolib.caelum.symboldb.CategoryExtractor;
import ru.prolib.caelum.symboldb.SymbolListRequest;
import ru.prolib.caelum.symboldb.SymbolService;
import ru.prolib.caelum.symboldb.SymbolUpdate;

public class FDBSymbolService implements SymbolService {
	private final Database db;
	private final CategoryExtractor catExt;
	private final FDBSchema schema;
	
	public FDBSymbolService(Database db, CategoryExtractor cat_ext, FDBSchema schema) {
		this.db = db;
		this.catExt = cat_ext;
		this.schema = schema;
	}
	
	@Override
	public void registerSymbol(String symbol) {
		db.run(new FDBTransactionRegisterSymbol(schema, catExt, symbol));
	}

	@Override
	public void registerSymbolUpdate(SymbolUpdate update) {
		db.run(new FDBTransactionRegisterSymbolUpdate(schema, catExt, update));
	}

	@Override
	public ICloseableIterator<String> listCategories() {
		return db.run(new FDBTransactionListCategories(schema));
	}
	
	@Override
	public ICloseableIterator<String> listSymbols(SymbolListRequest request) {
		return db.run(new FDBTransactionListSymbols(schema, request));
	}
	
	@Override
	public ICloseableIterator<SymbolUpdate> listSymbolUpdates(String symbol) {
		return db.run(new FDBTransactionListSymbolUpdates(schema, symbol));
	}

}
