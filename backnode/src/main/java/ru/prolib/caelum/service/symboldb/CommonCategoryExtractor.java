package ru.prolib.caelum.service.symboldb;

import java.util.Arrays;
import java.util.Collection;

public class CommonCategoryExtractor implements ICategoryExtractor {
	private static final CommonCategoryExtractor instance = new CommonCategoryExtractor();
	
	public static CommonCategoryExtractor getInstance() {
		return instance;
	}

	@Override
	public Collection<String> extract(String symbol) {
		int p = symbol.indexOf('@');
		if ( p <= 0 ) {
			return Arrays.asList("");
		} else {
			return Arrays.asList(symbol.substring(0, p));
		}
	}
	
	@Override
	public int hashCode() {
		return 1726900123;
	}
	
	@Override
	public boolean equals(Object other) {
		if ( other == this ) {
			return true;
		}
		if ( other == null || other.getClass() != CommonCategoryExtractor.class ) {
			return false;
		}
		return true;
	}

}
