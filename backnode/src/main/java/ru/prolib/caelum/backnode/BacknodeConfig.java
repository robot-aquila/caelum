package ru.prolib.caelum.backnode;

import java.util.Arrays;

import ru.prolib.caelum.backnode.rest.jetty.JettyServerBuilder;
import ru.prolib.caelum.lib.AbstractConfig;
import ru.prolib.caelum.service.itesym.ItesymBuilder;

public class BacknodeConfig extends AbstractConfig {
	public static final String DEFAULT_CONFIG_FILE		= "app.backnode.properties";
	public static final String HTTP_HOST	= "caelum.backnode.rest.http.host";
	public static final String HTTP_PORT	= "caelum.backnode.rest.http.port";
	public static final String MODE			= "caelum.backnode.mode";
	public static final String MODE_TEST = "test", MODE_PROD = "prod";

	@Override
	protected String getDefaultConfigFile() {
		return DEFAULT_CONFIG_FILE;
	}

	@Override
	protected void setDefaults() {
		props.put(HTTP_HOST, "localhost");
		props.put(HTTP_PORT, "9698");
		props.put(MODE, MODE_PROD);
		
		// Default extensions
		props.put("caelum.extension.builder.001_Itesym", ItesymBuilder.class.getName());
		props.put("caelum.extension.enabled.001_Itesym", "true");
		
		props.put("caelum.extension.builder.010_REST", RestServiceBuilder.class.getName());
		props.put("caelum.extension.enabled.010_REST", "true");
		
		props.put("caelum.extension.builder.900_HTTP", JettyServerBuilder.class.getName());
		props.put("caelum.extension.enabled.900_HTTP", "true");
	}
	
	public String getRestHttpHost() {
		return getString(HTTP_HOST);
	}
	
	public int getRestHttpPort() {
		return getInt(HTTP_PORT);
	}
	
	public boolean isTestMode() {
		return MODE_TEST.equals(getOneOfList(MODE, Arrays.asList(MODE_PROD, MODE_TEST)));
	}

}
