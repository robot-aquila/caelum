package ru.prolib.caelum.service;

import java.util.Map;
import java.util.concurrent.ExecutorService;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import ru.prolib.caelum.symboldb.ISymbolService;

public class SymbolCache implements ISymbolCache {
	private final ISymbolService symbolService;
	private final ExecutorService executor;
	private final Map<String, Boolean> markers;
	
	public SymbolCache(ISymbolService symbolService, ExecutorService executor, Map<String, Boolean> markers) {
		this.symbolService = symbolService;
		this.executor = executor;
		this.markers = markers;
	}
	
	public ISymbolService getSymbolService() {
		return symbolService;
	}
	
	public ExecutorService getExecutor() {
		return executor;
	}
	
	public Map<String, Boolean> getMarkers() {
		return markers;
	}

	@Override
	public void registerSymbol(String symbol) {
		if ( markers.containsKey(symbol) == false ) {
			executor.execute(new RegisterSymbol(symbol, symbolService, this));
		}
	}

	@Override
	public void setRegistered(String symbol) {
		markers.put(symbol, true);
	}
	
	@Override
	public void clear() {
		markers.clear();
	}
	
	@Override
	public int hashCode() {
		return new HashCodeBuilder(9861653, 37)
				.append(symbolService)
				.append(executor)
				.append(markers)
				.build();
	}
	
	@Override
	public boolean equals(Object other) {
		if  ( other == this ) {
			return true;
		}
		if ( other == null || other.getClass() != SymbolCache.class ) {
			return false;
		}
		SymbolCache o = (SymbolCache) other;
		return new EqualsBuilder()
				.append(o.symbolService, symbolService)
				.append(o.executor, executor)
				.append(o.markers, markers)
				.build();
	}

}
