package ru.prolib.caelum.symboldb;

import java.util.Collection;

public interface CategoryExtractor {
	Collection<String> extract(String symbol);
}
