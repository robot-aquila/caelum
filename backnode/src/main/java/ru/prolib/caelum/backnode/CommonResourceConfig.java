package ru.prolib.caelum.backnode;

import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;

import ru.prolib.caelum.backnode.exception.ThrowableMapper;
import ru.prolib.caelum.backnode.exception.WebApplicationExceptionMapper;

public class CommonResourceConfig extends ResourceConfig {
	
	public CommonResourceConfig() {
		register(WebApplicationExceptionMapper.class);
		register(ThrowableMapper.class);
		register(JacksonFeature.class);
	}

}
