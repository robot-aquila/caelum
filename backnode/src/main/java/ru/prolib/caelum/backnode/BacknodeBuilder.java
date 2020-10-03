package ru.prolib.caelum.backnode;

import java.io.IOException;

import ru.prolib.caelum.lib.IService;
import ru.prolib.caelum.service.BuildingContext;
import ru.prolib.caelum.service.CaelumBuilder;
import ru.prolib.caelum.service.GeneralConfigImpl;
import ru.prolib.caelum.service.ServiceRegistry;
import ru.prolib.caelum.service.ServletRegistry;

public class BacknodeBuilder {
	
	protected CaelumBuilder createCaelumBuilder() {
		return new CaelumBuilder();
	}
	
	protected ServiceRegistry createServiceRegistry() {
		return new ServiceRegistry();
	}
	
	protected ServletRegistry createServletRegistry() {
		return new ServletRegistry();
	}
	
	public IService build(String configFileName) throws IOException {
		ServiceRegistry services = createServiceRegistry();
		createCaelumBuilder().build(new BuildingContext(null, GeneralConfigImpl.DEFAULT_CONFIG_FILE,
				configFileName, null, services, createServletRegistry()));
		return services.getServices();
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
