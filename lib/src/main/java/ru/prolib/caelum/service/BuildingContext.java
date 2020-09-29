package ru.prolib.caelum.service;

import javax.servlet.Servlet;

import ru.prolib.caelum.lib.AbstractConfig;
import ru.prolib.caelum.lib.CompositeService;
import ru.prolib.caelum.lib.IService;

public class BuildingContext {
	private final CompositeService services;
	private final ServletRegistry servlets;
	private final String defaultConfigFileName, configFileName;
	private final AbstractConfig config;
	private final ICaelum caelum;
	
	public BuildingContext(AbstractConfig config,
			String defaultConfigFileName,
			String configFileName,
			ICaelum caelum,
			CompositeService services,
			ServletRegistry servlets)
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
	
	public BuildingContext(AbstractConfig config, String defaultConfigFileName, String configFileName, ICaelum caelum) {
		this(config, defaultConfigFileName, configFileName, caelum, new CompositeService(), new ServletRegistry());
	}
	
	public BuildingContext(AbstractConfig config, String defaultConfigFileName, String configFileName) {
		this(config, defaultConfigFileName, configFileName, null);
	}
	
	public BuildingContext withCaelum(ICaelum caelum) {
		return new BuildingContext(config, defaultConfigFileName, configFileName, caelum);
	}
	
	public ICaelum getCaelum() {
		if ( caelum == null ) {
			throw new IllegalStateException("Caelum service was not defined");
		}
		return caelum;
	}
	
	/**
	 * Get loaded config.
	 * <p>
	 * This method returns abstract config loaded using {@link AbstractConfig#load(String, String)} method with default
	 * config file name equal to {@link #getDefaultConfigFileName()} and config file name equal to
	 * {@link #getConfigFileName()}. The config can be used for any purposes with taking into account one
	 * thinnes: overriding config variable with system property or environment variable will work only in case if
	 * config contains property name to be overriden. So it should be defined either in default config or config
	 * overrides file. If no appropriate property defined at the moment before overriding with system property or
	 * environment property it will not work.  
	 * <p>
	 * @return
	 */
	public AbstractConfig getConfig() {
		return config;
	}
	
	/**
	 * Get config file name that used to load default values.
	 * <p>
	 * @return default config file name. This file cannot be null.
	 */
	public String getDefaultConfigFileName() {
		return defaultConfigFileName;
	}
	
	/**
	 * Get config file name that used to override defaults.
	 * <p>
	 * @return config file name. This file name may be null. That means default values were not overriden.
	 */
	public String getConfigFileName() {
		return configFileName;
	}
	
	public CompositeService getServices() {
		return services;
	}
	
	public ServletRegistry getServlets() {
		return servlets;
	}
	
	public BuildingContext registerService(IService service) {
		services.register(service);
		return this;
	}
	
	public BuildingContext registerServlet(Servlet servlet, String pathSpec) {
		servlets.registerServlet(servlet, pathSpec);
		return this;
	}
	
	public BuildingContext registerServlet(ServletMapping servlet) {
		servlets.registerServlet(servlet);
		return this;
	}
	
}
