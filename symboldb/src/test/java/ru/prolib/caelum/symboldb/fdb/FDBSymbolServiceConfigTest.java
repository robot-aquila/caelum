package ru.prolib.caelum.symboldb.fdb;

import static org.junit.Assert.*;

import java.util.Properties;

import org.junit.Before;
import org.junit.Test;

import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;

public class FDBSymbolServiceConfigTest {
	FDBSymbolServiceConfig service;

	@Before
	public void setUp() throws Exception {
		service = new FDBSymbolServiceConfig();
	}
	
	void verifyDefaultProperties(Properties props) {
		assertEquals("ru.prolib.caelum.symboldb.fdb.FDBSymbolServiceBuilder", props.get("caelum.symboldb.builder"));
		assertEquals("ru.prolib.caelum.symboldb.CommonCategoryExtractor", props.get("caelum.symboldb.category.extractor"));
		assertEquals("5000", props.get("caelum.symboldb.list.symbols.limit"));
		assertEquals("caelum", props.get("caelum.symboldb.fdb.subspace"));
		assertEquals("", props.get("caelum.symboldb.fdb.cluster"));
	}
	
	@Test
	public void testDefaults() throws Exception {
		assertEquals("app.symboldb.properties", FDBSymbolServiceConfig.DEFAULT_CONFIG_FILE);
		assertEquals("app.symboldb.properties", service.getDefaultConfigFile());
		
		verifyDefaultProperties(service.getProperties());
		
		Properties props = new Properties();
		assertTrue(service.loadFromResources(FDBSymbolServiceConfig.DEFAULT_CONFIG_FILE, props));
		
		verifyDefaultProperties(props);
	}

	@Test
	public void testGetSpace() {
		Subspace actual = service.getSpace();
		
		Subspace expected = new Subspace(Tuple.from("caelum"));
		assertEquals(expected, actual);
	}

}
