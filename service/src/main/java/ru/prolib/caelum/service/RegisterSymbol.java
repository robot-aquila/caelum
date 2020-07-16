package ru.prolib.caelum.service;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import ru.prolib.caelum.symboldb.ISymbolService;

public class RegisterSymbol implements Runnable {
	private final String symbol;
	private final ISymbolService symbolService;
	private final ISymbolCache symbolCache;
	
	public RegisterSymbol(String symbol, ISymbolService symbolService, ISymbolCache symbolCache) {
		this.symbol = symbol;
		this.symbolService = symbolService;
		this.symbolCache = symbolCache;
	}
	
	public String getSymbol() {
		return symbol;
	}
	
	public ISymbolService getSymbolService() {
		return symbolService;
	}
	
	public ISymbolCache getSymbolCache() {
		return symbolCache;
	}

	@Override
	public void run() {
		symbolService.registerSymbol(symbol);
		symbolCache.setRegistered(symbol);
	}
	
	@Override
	public int hashCode() {
		return new HashCodeBuilder(117295, 13)
				.append(symbol)
				.append(symbolService)
				.append(symbolCache)
				.build();
	}
	
	@Override
	public boolean equals(Object other) {
		if ( other == this ) {
			return true;
		}
		if ( other == null || other.getClass() != RegisterSymbol.class ) {
			return false;
		}
		RegisterSymbol o = (RegisterSymbol) other;
		return new EqualsBuilder()
				.append(o.symbol, symbol)
				.append(o.symbolService, symbolService)
				.append(o.symbolCache, symbolCache)
				.build();
	}

}
