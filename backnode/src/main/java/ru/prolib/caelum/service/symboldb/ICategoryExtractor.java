package ru.prolib.caelum.service.symboldb;

import java.util.Collection;

public interface ICategoryExtractor {
	Collection<String> extract(String symbol);
}
