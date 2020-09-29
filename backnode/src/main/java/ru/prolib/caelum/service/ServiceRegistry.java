package ru.prolib.caelum.service;

import ru.prolib.caelum.lib.CompositeService;
import ru.prolib.caelum.lib.IService;

public class ServiceRegistry implements IServiceRegistry {
	private final CompositeService services;
	
	public ServiceRegistry(CompositeService services) {
		this.services = services;
	}
	
	public ServiceRegistry() {
		this(new CompositeService());
	}
	
	public CompositeService getServices() {
		return services;
	}

	@Override
	public IServiceRegistry registerService(IService service) {
		services.register(service);
		return this;
	}

}
