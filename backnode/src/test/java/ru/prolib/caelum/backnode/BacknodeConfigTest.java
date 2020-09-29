package ru.prolib.caelum.backnode;

import static org.junit.Assert.*;

import java.util.Properties;

import org.junit.Before;
import org.junit.Test;

import ru.prolib.caelum.backnode.rest.jetty.JettyServerBuilder;
import ru.prolib.caelum.service.itesym.ItesymBuilder;

public class BacknodeConfigTest {
	BacknodeConfig service;

	@Before
	public void setUp() throws Exception {
		service = new BacknodeConfig();
	}
	
	void verifyDefaultProperties(Properties props) {
		assertEquals("localhost", props.get("caelum.backnode.rest.http.host"));
		assertEquals("9698", props.get("caelum.backnode.rest.http.port"));
		assertEquals("prod", props.get("caelum.backnode.mode"));
		
		// Extensions
		assertEquals(ItesymBuilder.class.getName(), props.get("caelum.extension.builder.001_Itesym"));
		assertEquals("true", props.get("caelum.extension.enabled.001_Itesym"));
		
		assertEquals(RestServiceBuilder.class.getName(), props.get("caelum.extension.builder.010_REST"));
		assertEquals("true", props.get("caelum.extension.enabled.010_REST"));
		
		assertEquals(JettyServerBuilder.class.getName(), props.get("caelum.extension.builder.900_HTTP"));
		assertEquals("true", props.get("caelum.extension.enabled.900_HTTP"));
	}

	@Test
	public void testDefaults() throws Exception {
		assertEquals("app.backnode.properties", BacknodeConfig.DEFAULT_CONFIG_FILE);
		assertEquals("app.backnode.properties", service.getDefaultConfigFile());
		
		verifyDefaultProperties(service.getProperties());
		
		Properties props = new Properties();
		assertTrue(service.loadFromResources(BacknodeConfig.DEFAULT_CONFIG_FILE, props));
		
		verifyDefaultProperties(props);
	}
	
	@Test
	public void testGetRestHttpHost() {
		service.getProperties().put("caelum.backnode.rest.http.host", "tutumbr97");
		
		assertEquals("tutumbr97", service.getRestHttpHost());
	}
	
	@Test
	public void testGetRestHttpPort() {
		service.getProperties().put("caelum.backnode.rest.http.port", "708");
		
		assertEquals(708, service.getRestHttpPort());
	}
	
	@Test
	public void testIsTestMode() {
		assertFalse(service.isTestMode());
		
		service.getProperties().put("caelum.backnode.mode", "test");
		assertTrue(service.isTestMode());
		
		service.getProperties().put("caelum.backnode.mode", "prod");
		assertFalse(service.isTestMode());
	}
	
}
