package ru.prolib.caelum.backnode.rest;

import java.io.IOException;

import ru.prolib.caelum.core.IService;
import ru.prolib.caelum.service.ICaelum;

public interface IRestServiceBuilder {
	IService build(String default_config_file, String config_file, ICaelum caelum) throws IOException;
}
