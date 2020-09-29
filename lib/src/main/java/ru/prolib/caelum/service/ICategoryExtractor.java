package ru.prolib.caelum.service;

import java.util.Collection;

public interface ICategoryExtractor {
	Collection<String> extract(String symbol);
}
