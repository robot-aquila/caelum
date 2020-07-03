package ru.prolib.caelum.symboldb.fdb;

import com.apple.foundationdb.Database;

import ru.prolib.caelum.core.ICloseableIterator;
import ru.prolib.caelum.symboldb.ICategoryExtractor;
import ru.prolib.caelum.symboldb.SymbolListRequest;
import ru.prolib.caelum.symboldb.ISymbolService;
import ru.prolib.caelum.symboldb.SymbolUpdate;

public class FDBSymbolService implements ISymbolService {
	private final Database db;
	private final ICategoryExtractor catExt;
	private final FDBSchema schema;
	
	public FDBSymbolService(Database db, ICategoryExtractor cat_ext, FDBSchema schema) {
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
