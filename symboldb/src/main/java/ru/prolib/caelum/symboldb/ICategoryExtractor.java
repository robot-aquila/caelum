package ru.prolib.caelum.symboldb;

import java.util.Collection;

public interface ICategoryExtractor {
	Collection<String> extract(String symbol);
}
