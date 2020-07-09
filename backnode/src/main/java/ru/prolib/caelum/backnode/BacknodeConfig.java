package ru.prolib.caelum.backnode;

import ru.prolib.caelum.core.AbstractConfig;

public class BacknodeConfig extends AbstractConfig {
	public static final String DEFAULT_CONFIG_FILE		= "app.backnode.properties";
	public static final String HTTP_HOST	= "caelum.backnode.rest.http.host";
	public static final String HTTP_PORT	= "caelum.backnode.rest.http.port";

	@Override
	protected String getDefaultConfigFile() {
		return DEFAULT_CONFIG_FILE;
	}

	@Override
	protected void setDefaults() {
		props.put(HTTP_HOST, "localhost");
		props.put(HTTP_PORT, "9698");
	}
	
	public String getRestHttpHost() {
		return getString(HTTP_HOST);
	}
	
	public int getRestHttpPort() {
		return getInt(HTTP_PORT);
	}

}
