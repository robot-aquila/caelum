package ru.prolib.caelum.service;

import java.io.IOException;

import ru.prolib.caelum.core.CompositeService;

public interface IExtensionBuilder {
	IExtension build(String default_config_file, String config_file, CompositeService services, ICaelum caelum)
		throws IOException;
}
