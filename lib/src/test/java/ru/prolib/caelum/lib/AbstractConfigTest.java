package ru.prolib.caelum.lib;

import static org.junit.Assert.*;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.MatcherAssert.assertThat;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;

public class AbstractConfigTest {
	
	static class TestConfig extends AbstractConfig {
		static final String PROPERTY1 = "testconfig.property1";
		static final String PROPERTY2 = "testconfig.property2";
		static final String PROPERTY3 = "testconfig.property3";

		@Override
		protected void setDefaults() {
			props.put(PROPERTY1, "foobar");
			props.put(PROPERTY2, "15");
			props.put(PROPERTY3, "false");
		}

		@Override
		protected String getDefaultConfigFile() {
			return "testconfig1.properties";
		}
		
	}

	@Rule
	public final EnvironmentVariables env = new EnvironmentVariables();
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
	public void testLoad2_OverrideFromFileInResources() throws Exception {
		service.load("testconfig1.properties", null);
		
		assertEquals("Hello, World!", service.getString(TestConfig.PROPERTY1));
		assertEquals(15, service.getInt(TestConfig.PROPERTY2)); // shouldn't be overridden
		assertEquals("true", service.getString(TestConfig.PROPERTY3));
	}
	
	@Test
	public void testLoad2_OverrideByFileInCWD() throws Exception {
		service.load("testconfig2.properties", null);
		
		assertEquals("umbabarauma", service.getString(TestConfig.PROPERTY1)); // should be loaded from resources
		assertEquals(42, service.getInt(TestConfig.PROPERTY2)); // from file in CWD
		assertEquals("false", service.getString(TestConfig.PROPERTY3));
	}
	
	@Test
	public void testLoad2_OverrideBySpecificFile() throws Exception {
		service.load("testconfig2.properties", "testconfig2-overriden.properties");
		
		assertEquals("kobresia", service.getString(TestConfig.PROPERTY1));
		assertEquals(15, service.getInt(TestConfig.PROPERTY2));
		assertEquals("1", service.getString(TestConfig.PROPERTY3));
	}

	@Test
	public void testLoad2_OverrideBySystemProperties() throws Exception {
		System.setProperty(TestConfig.PROPERTY2, "146");
		System.setProperty(TestConfig.PROPERTY3, "unknown");
		
		service.load("testconfig2.properties", "testconfig2-overriden.properties");
		
		assertEquals("kobresia", service.getString(TestConfig.PROPERTY1));
		assertEquals(146, service.getInt(TestConfig.PROPERTY2));
		assertEquals("unknown", service.getString(TestConfig.PROPERTY3));
	}
	
	@Test
	public void testLoad2_OverrideBySpecificFile_UndefinedProperty() throws Exception {
		service.load("testconfig2.properties", "testconfig3.properties");
		
		assertEquals("", service.getString(TestConfig.PROPERTY1));
	}
	
	@Test
	public void testLoad2_OverrideByEnvironmentVariable() throws Exception {
		System.setProperty(TestConfig.PROPERTY2, "146");
		System.setProperty(TestConfig.PROPERTY3, "unknown");
		env.set(TestConfig.PROPERTY2, "88827");
		env.set(TestConfig.PROPERTY3, "karamba");
		
		service.load("testconfig2.properties", "testconfig2-overriden.properties");
		
		assertEquals("kobresia", service.getString(TestConfig.PROPERTY1));
		assertEquals(88827, service.getInt(TestConfig.PROPERTY2));
		assertEquals("karamba", service.getString(TestConfig.PROPERTY3));
	}
	
	@Test
	public void testLoad1() throws Exception {
		service.load("testconfig2.properties");
		
		assertEquals("Hello, World!", service.getString(TestConfig.PROPERTY1));
		assertEquals(42, service.getInt(TestConfig.PROPERTY2));
		assertEquals("true", service.getString(TestConfig.PROPERTY3));
	}
	
	@Test
	public void testGetBoolean() {
		assertNull(service.getBoolean("tumbe"));
		assertEquals(true, service.getBoolean("tumbe", true));
		assertEquals(false, service.getBoolean("tumbe", false));

		service.getProperties().put("tumbe", "");
		assertNull(service.getBoolean("tumbe"));
		assertEquals(true, service.getBoolean("tumbe", true));
		assertEquals(false, service.getBoolean("tumbe", false));
		
		service.getProperties().put("tumbe", "1");
		assertEquals(true, service.getBoolean("tumbe"));
		assertEquals(true, service.getBoolean("tumbe", true));
		assertEquals(true, service.getBoolean("tumbe", false));
		
		service.getProperties().put("tumbe", "true");
		assertEquals(true, service.getBoolean("tumbe"));
		assertEquals(true, service.getBoolean("tumbe", true));
		assertEquals(true, service.getBoolean("tumbe", false));
		
		service.getProperties().put("tumbe", "0");
		assertEquals(false, service.getBoolean("tumbe"));
		assertEquals(false, service.getBoolean("tumbe", true));
		assertEquals(false, service.getBoolean("tumbe", false));
		
		service.getProperties().put("tumbe", "false");
		assertEquals(false, service.getBoolean("tumbe"));
		assertEquals(false, service.getBoolean("tumbe", true));
		assertEquals(false, service.getBoolean("tumbe", false));
	}
	
	@Test
	public void testGetBoolean_ShouldThrowIfUnsupportedValue() {
		service.getProperties().put("tumbe", "solution");
		
		IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> service.getBoolean("tumbe"));
		assertEquals("Expected boolean type of key: tumbe but value is: solution", e.getMessage());
	}
	
	@Test
	public void testGetLong1() {
		service.getProperties().put("umbra", "31536000000000");
		
		assertEquals(31536000000000L, service.getLong("umbra"));
	}
	
	@Test
	public void testGetShort1() {
		service.getProperties().put("karamba", "210");
		
		short expected = 210;
		assertEquals(expected, service.getShort("karamba"));
	}
	
	@Test
	public void testGetShort_ShouldThrowIfValueTooBig() {
		service.getProperties().put("karamba", "99999");
		NumberFormatException e = assertThrows(NumberFormatException.class, () -> service.getShort("karamba"));
		assertThat(e.getMessage(), is(startsWith("Value out of range")));
	}

}
