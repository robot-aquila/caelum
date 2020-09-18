package ru.prolib.caelum.symboldb;

import static org.junit.Assert.*;

import java.util.Properties;

import org.junit.Before;
import org.junit.Test;

public class SymbolServiceConfigTest {
	SymbolServiceConfig service;

	@Before
	public void setUp() throws Exception {
		service = new SymbolServiceConfig();
	}
	
	void verifyDefaultProperties(Properties props) {
		assertEquals("ru.prolib.caelum.symboldb.fdb.FDBSymbolServiceBuilder", props.get("caelum.symboldb.builder"));
		assertEquals("ru.prolib.caelum.symboldb.CommonCategoryExtractor", props.get("caelum.symboldb.category.extractor"));
		assertEquals("5000", props.get("caelum.symboldb.list.symbols.limit"));
		assertEquals("5000", props.get("caelum.symboldb.list.events.limit"));
	}

	@Test
	public void testDefaults() throws Exception {
		assertEquals("app.symboldb.properties", SymbolServiceConfig.DEFAULT_CONFIG_FILE);
		assertEquals("app.symboldb.properties", service.getDefaultConfigFile());
		
		verifyDefaultProperties(service.getProperties());
		
		Properties props = new Properties();
		assertTrue(service.loadFromResources(SymbolServiceConfig.DEFAULT_CONFIG_FILE, props));
		
		verifyDefaultProperties(props);
	}

}
