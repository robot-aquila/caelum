package ru.prolib.caelum.core;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;

public abstract class AbstractConfig {
	protected Properties props = new Properties();
	
	public AbstractConfig() {
		setDefaults();
	}
	
	protected abstract void setDefaults();
	
	/**
	 * Return path to default configuration properties file.
	 * The system will search for this file in resources, then in current directory.
	 * <p>
	 * @return path
	 */
	protected abstract String getDefaultConfigFile();
	
	public boolean loadFromResources(String path, Properties props) throws IOException {
		InputStream is = getClass().getClassLoader().getResourceAsStream(path);
		if ( is == null ) return false;
		try {
			props.load(is);
		} finally {
			is.close();
		}
		return true;
	}
	
	public boolean loadFromResources(String path) throws IOException {
		return loadFromResources(path, props);
	}
	
	public boolean loadFromFile(String path) throws IOException {
		File file = new File(path);
		if ( ! file.exists() ) return false;
		try ( InputStream _is = new FileInputStream(file) ) {
			props.load(_is);
		}
		return true;
	}
	
	public void loadFromSystemProperties() throws IOException {
		Properties sys_props = System.getProperties();
		for ( String key : props.stringPropertyNames() ) {
			if ( sys_props.containsKey(key) ) {
				props.put(key, sys_props.getProperty(key));
			}
		}
	}
	
	private void loadFromEnvironmentVariables() {
		Map<String, String> env_props = System.getenv();
		for ( String key : props.stringPropertyNames() ) {
			if ( env_props.containsKey(key) ) {
				props.put(key,  env_props.get(key));
			}
		}
	}
	
	/**
	 * Load configuration from all available sources.
	 * <p>
	 * Loading priority:
	 * <li>Define default properties in constructor</li>
	 * <li>From properties file {@code default_config_file} in resources if exists</li>
	 * <li>From properties file {@code default_config_file} in current directory
	 * if {@code config_file} is not specified</li>
	 * <li>From properties file {@code config_file}. If file is not exist the exception will be thrown</li>
	 * <li>From system properties. That allows to override property using -Dproperty.name command line
	 * option while running java</li>
	 * <li>From environment variables</li>
	 * <p>
	 * @param default_config_file - default configuration file.
	 * @param config_file - specific configuration properties file to load from (i.g. obtained from args).
	 * If null then loading from specific file will be omited. 
	 * @throws IOException - an error occurred
	 */
	public void load(String default_config_file, String config_file) throws IOException {
		loadFromResources(default_config_file);
		if ( config_file != null ) {
			if ( ! loadFromFile(config_file) ) {
				throw new IOException("Error loading config: " + config_file);
			}
		} else {
			loadFromFile(default_config_file);
		}
		loadFromSystemProperties();
		loadFromEnvironmentVariables();
	}
	
	public void load(String config_file) throws IOException {
		load(getDefaultConfigFile(), config_file);
	}
	
	public void print(PrintStream stream) {
		List<String> keys = new ArrayList<>(props.stringPropertyNames());
		Collections.sort(keys);
		for ( String key : keys ) {
			stream.println(key + "=" + props.getProperty(key));
		}
	}
	
	public void print(Logger logger) {
		List<String> keys = new ArrayList<>(props.stringPropertyNames());
		Collections.sort(keys);
		for ( String key : keys ) {
			logger.debug("\t" + key + "=" + props.getProperty(key));
		}
	}
	
	public Properties getProperties() {
		return props;
	}
	
	public String getString(String key) {
		String val = props.getProperty(key);
		if ( val == null ) {
			throw new NullPointerException("Value not configured: " + key);
		}
		return val;
	}
	
	public int getInt(String key) {
		return Integer.valueOf(getString(key));
	}
	
	public int getInt(String key, int min, int max) {
		int val = getInt(key);
		if ( val < min || val > max ) {
			throw new NumberFormatException(key + "expected to be in range " + min + "-" + max + " but: " + val);
		}
		return val;
	}
	
	public long getLong(String key) {
		return Long.valueOf(getString(key));
	}
	
	public short getShort(String key) {
		return Short.valueOf(getString(key));
	}
	
	public String getOneOfList(String key, List<String> list) {
		String val = getString(key);
		if ( ! list.contains(val) ) {
			throw new IllegalStateException(key + " expected to be one of " + list + " but: " + val);
		}
		return val;
	}
	
	public Boolean getBoolean(String key) {
		String str_val = props.getProperty(key);
		if ( str_val == null || "".equals(str_val) ) {
			return null;
		}
		switch ( str_val ) {
		case "1":
		case "true":
			return true;
		case "0":
		case "false":
			return false;
		default:
			throw new IllegalArgumentException("Expected boolean type of key: " + key + " but value is: " + str_val);
		}
	}
	
	public boolean getBoolean(String key, boolean default_value) {
		Boolean val = getBoolean(key);
		return val == null ? default_value : val;
	}
	
}
