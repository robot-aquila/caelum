package ru.prolib.caelum.service.symboldb;

import java.util.Collection;

import ru.prolib.caelum.lib.Events;
import ru.prolib.caelum.lib.ICloseableIterator;

public interface ISymbolService {
	void registerSymbol(String symbol);
	void registerSymbol(Collection<String> symbols);
	void registerEvents(Events events);
	void registerEvents(Collection<Events> events);
	void deleteEvents(Events events);
	void deleteEvents(Collection<Events> events);
	ICloseableIterator<String> listCategories();
	ICloseableIterator<String> listSymbols(SymbolListRequest request);
	ICloseableIterator<Events> listEvents(EventListRequest request);
	void clear(boolean global);
}
