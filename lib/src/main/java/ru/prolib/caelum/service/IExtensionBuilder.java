package ru.prolib.caelum.service;

import java.io.IOException;

import ru.prolib.caelum.lib.CompositeService;

public interface IExtensionBuilder {
	IExtension build(String x, String y, CompositeService s, ICaelum c) throws IOException;
}
