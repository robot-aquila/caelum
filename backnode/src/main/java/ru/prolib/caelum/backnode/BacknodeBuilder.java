package ru.prolib.caelum.backnode;

import java.io.IOException;

import ru.prolib.caelum.backnode.rest.IRestServiceBuilder;
import ru.prolib.caelum.backnode.rest.jetty.JettyServerBuilder;
import ru.prolib.caelum.lib.CompositeService;
import ru.prolib.caelum.lib.IService;
import ru.prolib.caelum.service.CaelumBuilder;
import ru.prolib.caelum.service.ICaelum;

public class BacknodeBuilder {
	
	protected CompositeService createServices() {
		return new CompositeService();
	}
	
	protected CaelumBuilder createCaelumBuilder() {
		return new CaelumBuilder();
	}
	
	protected IRestServiceBuilder createRestServerBuilder() {
		return new JettyServerBuilder();
	}
	
	public IService build(String config_file) throws IOException {
		final String default_config_file = BacknodeConfig.DEFAULT_CONFIG_FILE;
		CompositeService services = createServices();
		ICaelum caelum = createCaelumBuilder().build(default_config_file, config_file, services);
		services.register(createRestServerBuilder().build(default_config_file, config_file, caelum));
		return services;
	}
	
	@Override
	public int hashCode() {
		return 8263811;
	}
	
	@Override
	public boolean equals(Object other) {
		if ( other == this ) {
			return true;
		}
		if ( other == null || other.getClass() != BacknodeBuilder.class ) {
			return false;
		}
		return true;
	}

}
