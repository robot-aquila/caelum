package ru.prolib.caelum.core;

import static org.junit.Assert.*;

import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class AbstractConfigTest {
	
	static class TestConfig extends AbstractConfig {
		static final String PROPERTY1 = "testconfig.property1";
		static final String PROPERTY2 = "testconfig.property2";
		static final String PROPERTY3 = "testconfig.property3";

		@Override
		public void setDefaults() {
			props.put(PROPERTY1, "foobar");
			props.put(PROPERTY2, "15");
			props.put(PROPERTY3, "false");
		}

		@Override
		public Properties getKafkaProperties() {
			throw new UnsupportedOperationException();
		}
		
	}

	TestConfig service;
	
	@Before
	public void setUp() throws Exception {
		service = new TestConfig();
	}
	
	@After
	public void tearDown() throws Exception {
		System.clearProperty(TestConfig.PROPERTY1);
		System.clearProperty(TestConfig.PROPERTY2);
		System.clearProperty(TestConfig.PROPERTY3);
	}
	
	@Test
	public void testDefaults() {
		assertEquals("foobar", service.getString(TestConfig.PROPERTY1));
		assertEquals(15, service.getInt(TestConfig.PROPERTY2));
		assertEquals("false", service.getString(TestConfig.PROPERTY3));
	}
	
	@Test
	public void testLoad_OverrideFromFileInResources() throws Exception {
		service.load("testconfig1.properties", null);
		
		assertEquals("Hello, World!", service.getString(TestConfig.PROPERTY1));
		assertEquals(15, service.getInt(TestConfig.PROPERTY2)); // shouldn't be overridden
		assertEquals("true", service.getString(TestConfig.PROPERTY3));
	}
	
	@Test
	public void testLoad_OverrideByFileInCWD() throws Exception {
		service.load("testconfig2.properties", null);
		
		assertEquals("umbabarauma", service.getString(TestConfig.PROPERTY1)); // should be loaded from resources
		assertEquals(42, service.getInt(TestConfig.PROPERTY2)); // from file in CWD
		assertEquals("false", service.getString(TestConfig.PROPERTY3));
	}
	
	@Test
	public void testLoad_OverrideBySpecificFile() throws Exception {
		service.load("testconfig2.properties", "testconfig2-overriden.properties");
		
		assertEquals("kobresia", service.getString(TestConfig.PROPERTY1));
		assertEquals(15, service.getInt(TestConfig.PROPERTY2));
		assertEquals("1", service.getString(TestConfig.PROPERTY3));
	}

	@Test
	public void testLoad_OverrideBySystemProperties() throws Exception {
		System.setProperty(TestConfig.PROPERTY2, "146");
		System.setProperty(TestConfig.PROPERTY3, "unknown");

		service.load("testconfig2.properties", "testconfig2-overriden.properties");
		
		assertEquals("kobresia", service.getString(TestConfig.PROPERTY1));
		assertEquals(146, service.getInt(TestConfig.PROPERTY2));
		assertEquals("unknown", service.getString(TestConfig.PROPERTY3));
	}
	
	@Test
	public void testLoad_OverrideBySpecificFile_UndefinedProperty() throws Exception {
		service.load("testconfig2.properties", "testconfig3.properties");
		
		assertEquals("", service.getString(TestConfig.PROPERTY1));
	}

}
