package ru.prolib.caelum.service.symboldb.fdb;

import java.util.Arrays;
import java.util.Collection;

import com.apple.foundationdb.Database;

import ru.prolib.caelum.lib.Events;
import ru.prolib.caelum.lib.ICloseableIterator;
import ru.prolib.caelum.service.symboldb.EventListRequest;
import ru.prolib.caelum.service.symboldb.ICategoryExtractor;
import ru.prolib.caelum.service.symboldb.ISymbolService;
import ru.prolib.caelum.service.symboldb.SymbolListRequest;

public class FDBSymbolService implements ISymbolService {
	private volatile Database db;
	private final ICategoryExtractor catExt;
	private final FDBSchema schema;
	private final int listSymbolsMaxLimit, listEventsMaxLimit;
	
	public FDBSymbolService(ICategoryExtractor cat_ext,
			FDBSchema schema,
			int list_symbols_max_limit,
			int list_events_max_limit)
	{
		this.db = db;
		this.catExt = cat_ext;
		this.schema = schema;
		this.listSymbolsMaxLimit = list_symbols_max_limit;
		this.listEventsMaxLimit = list_events_max_limit;
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
	
	public int getListEventsMaxLimit() {
		return listEventsMaxLimit;
	}
	
	@Override
	public void registerSymbol(String symbol) {
		db.run(new FDBTransactionRegisterSymbol(schema, catExt, Arrays.asList(symbol)));
	}
	
	@Override
	public void registerSymbol(Collection<String> symbols) {
		db.run(new FDBTransactionRegisterSymbol(schema, catExt, symbols));
	}

	@Override
	public void registerEvents(Events events) {
		db.run(new FDBTransactionRegisterEvents(schema, catExt, Arrays.asList(events)));
	}
	
	@Override
	public void registerEvents(Collection<Events> events) {
		db.run(new FDBTransactionRegisterEvents(schema, catExt, events));
	}
	
	@Override
	public void deleteEvents(Events events) {
		db.run(new FDBTransactionDeleteEvents(schema, Arrays.asList(events)));
	}
	
	@Override
	public void deleteEvents(Collection<Events> events) {
		db.run(new FDBTransactionDeleteEvents(schema, events));
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
	public ICloseableIterator<Events> listEvents(EventListRequest request) {
		return db.run(new FDBTransactionListEvents(schema, request, listEventsMaxLimit));
	}

	@Override
	public void clear(boolean global) {
		if ( global ) {
			db.run(new FDBTransactionClear(schema));
		}
	}

}
