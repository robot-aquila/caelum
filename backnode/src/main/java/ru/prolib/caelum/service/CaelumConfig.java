package ru.prolib.caelum.service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import ru.prolib.caelum.lib.AbstractConfig;

public class CaelumConfig extends AbstractConfig {
	public static final String DEFAULT_CONFIG_FILE = "app.caelum.properties";
	public static final String EXTENSION_BUILDER_PFX = "caelum.extension.builder.";
	public static final String EXTENSION_ENABLED_PFX = "caelum.extension.enabled.";

	@Override
	protected void setDefaults() {
		
	}

	@Override
	protected String getDefaultConfigFile() {
		return DEFAULT_CONFIG_FILE;
	}
	
	public List<ExtensionConf> getExtensions() {
		List<ExtensionConf> result = new ArrayList<>();
		for ( String key : props.stringPropertyNames() ) {
			if ( key.startsWith(EXTENSION_BUILDER_PFX) ) {
				String id = key.substring(EXTENSION_BUILDER_PFX.length());
				result.add(new ExtensionConf(id, props.getProperty(key), getBoolean(EXTENSION_ENABLED_PFX + id, true)));
			}
		}
		Collections.sort(result, (a, b) -> a.getId().compareTo(b.getId()) );
		return result;
	}

}
