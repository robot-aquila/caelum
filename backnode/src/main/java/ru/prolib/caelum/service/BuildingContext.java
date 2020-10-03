package ru.prolib.caelum.service;

import javax.servlet.Servlet;

import org.apache.commons.lang3.builder.EqualsBuilder;

import ru.prolib.caelum.lib.IService;

public class BuildingContext implements IBuildingContext {
	private final IServiceRegistry services;
	private final IServletRegistry servlets;
	private final String defaultConfigFileName, configFileName;
	private final GeneralConfig config;
	private final ICaelum caelum;
	
	public BuildingContext(GeneralConfig config,
			String defaultConfigFileName,
			String configFileName,
			ICaelum caelum,
			IServiceRegistry services,
			IServletRegistry servlets)
	{
		if ( defaultConfigFileName == null ) {
			throw new IllegalArgumentException("Default config file name cannot be null");
		}
		this.config = config;
		this.defaultConfigFileName = defaultConfigFileName;
		this.configFileName = configFileName;
		this.caelum = caelum;
		this.services = services;
		this.servlets = servlets;
	}
	
	public BuildingContext withCaelum(ICaelum caelum) {
		return new BuildingContext(config, defaultConfigFileName, configFileName, caelum, services, servlets);
	}
	
	public BuildingContext withConfig(GeneralConfig config) {
		return new BuildingContext(config, defaultConfigFileName, configFileName, caelum, services, servlets);
	}
	
	public IServiceRegistry getServices() {
		return services;
	}
	
	public IServletRegistry getServlets() {
		return servlets;
	}
	
	@Override
	public ICaelum getCaelum() {
		if ( caelum == null ) {
			throw new IllegalStateException("Caelum service was not defined");
		}
		return caelum;
	}
	
	@Override
	public GeneralConfig getConfig() {
		return config;
	}
	
	@Override
	public String getDefaultConfigFileName() {
		return defaultConfigFileName;
	}
	
	@Override
	public String getConfigFileName() {
		return configFileName;
	}
	
	@Override
	public IBuildingContext registerService(IService service) {
		services.registerService(service);
		return this;
	}
	
	@Override
	public IBuildingContext registerServlet(Servlet servlet, String pathSpec) {
		servlets.registerServlet(servlet, pathSpec);
		return this;
	}
	
	@Override
	public IBuildingContext registerServlet(ServletMapping servlet) {
		servlets.registerServlet(servlet);
		return this;
	}
	
	@Override
	public boolean equals(Object other) {
		if ( other == this ) {
			return true;
		}
		if ( other == null || other.getClass() != BuildingContext.class ) {
			return false;
		}
		BuildingContext o = (BuildingContext) other;
		return new EqualsBuilder()
				.append(o.config, config)
				.append(o.defaultConfigFileName, defaultConfigFileName)
				.append(o.configFileName, configFileName)
				.append(o.caelum, caelum)
				.append(o.services, services)
				.append(o.servlets, servlets)
				.build();
	}
	
}
