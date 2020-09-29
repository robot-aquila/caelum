package ru.prolib.caelum.service;

import javax.servlet.Servlet;

import ru.prolib.caelum.lib.AbstractConfig;
import ru.prolib.caelum.lib.IService;

public interface IBuildingContext {

	/**
	 * Get facade to Caelum system.
	 * <p>
	 * This method should be used only after Caelum instantiation. It can be used by extensions but Caelum subsystems.
	 * <p>
	 * @return Caelum facade instance
	 * @throws IllegalStateException - if facade instance not defined
	 */
	ICaelum getCaelum();

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
	 * @return config or null if no config loaded
	 */
	AbstractConfig getConfig();

	/**
	 * Get config file name that used to load default values.
	 * <p>
	 * @return default config file name. This file cannot be null.
	 */
	String getDefaultConfigFileName();

	/**
	 * Get config file name that used to override defaults.
	 * <p>
	 * @return config file name. This file name may be null. That means default values were not overriden.
	 */
	String getConfigFileName();

	IBuildingContext registerService(IService service);

	IBuildingContext registerServlet(Servlet servlet, String pathSpec);

	IBuildingContext registerServlet(ServletMapping servlet);

}