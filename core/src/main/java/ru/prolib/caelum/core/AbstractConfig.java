package ru.prolib.caelum.core;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.slf4j.Logger;

public abstract class AbstractConfig {
	protected Properties props = new Properties();
	
	public AbstractConfig() {
		setDefaults();
	}
	
	public abstract void setDefaults();
	public abstract Properties getKafkaProperties();
	
	public boolean loadFromResources(String path) throws IOException {
		InputStream is = getClass().getClassLoader().getResourceAsStream(path);
		if ( is == null ) return false;
		try {
			props.load(is);
		} finally {
			is.close();
		}
		return true;
	}
	
	public boolean loadFromFile(String path) throws IOException {
		File file = new File(path);
		if ( ! file.exists() ) return false;
		try ( InputStream _is = new FileInputStream(file) ) {
			props.load(_is);
		}
		return true;
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
			logger.info("\t" + key + "=" + props.getProperty(key));
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
	
	public String getOneOfList(String key, List<String> list) {
		String val = getString(key);
		if ( ! list.contains(val) ) {
			throw new IllegalStateException(key + " expected to be one of " + list + " but: " + val);
		}
		return val;
	}
	
}
