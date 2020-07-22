package ru.prolib.caelum.symboldb.fdb;

import com.apple.foundationdb.Database;

import ru.prolib.caelum.core.ICloseableIterator;
import ru.prolib.caelum.symboldb.ICategoryExtractor;
import ru.prolib.caelum.symboldb.SymbolListRequest;
import ru.prolib.caelum.symboldb.ISymbolService;
import ru.prolib.caelum.symboldb.SymbolUpdate;

public class FDBSymbolService implements ISymbolService {
	private volatile Database db;
	private final ICategoryExtractor catExt;
	private final FDBSchema schema;
	private final int listSymbolsMaxLimit;
	
	public FDBSymbolService(ICategoryExtractor cat_ext, FDBSchema schema, int list_symbols_max_limit) {
		this.db = db;
		this.catExt = cat_ext;
		this.schema = schema;
		this.listSymbolsMaxLimit = list_symbols_max_limit;
	}
	
	public void setDatabase(Database db) {
		this.db = db;
	}
	
	public Database getDatabase() {
		return db;
	}
	
	public ICategoryExtractor getCategoryExtractor() {
		return catExt;
	}
	
	public FDBSchema getSchema() {
		return schema;
	}
	
	public int getListSymbolsMaxLimit() {
		return listSymbolsMaxLimit;
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
		return db.run(new FDBTransactionListSymbols(schema, request, listSymbolsMaxLimit));
	}
	
	@Override
	public ICloseableIterator<SymbolUpdate> listSymbolUpdates(String symbol) {
		return db.run(new FDBTransactionListSymbolUpdates(schema, symbol));
	}

	@Override
	public void clear() {
		db.run(new FDBTransactionClear(schema));
	}

}
