package ru.prolib.caelum.itesym.ak;

import static org.junit.Assert.*;

import java.util.Properties;

import org.junit.Before;
import org.junit.Test;

public class ItesymConfigTest {
	ItesymConfig service;

	@Before
	public void setUp() throws Exception {
		service = new ItesymConfig();
	}
	
	void verifyDefaultProperties(Properties props) {
		assertEquals("localhost:8082",	props.get("caelum.itesym.bootstrap.servers"));
		assertEquals("caelum-itesym",	props.get("caelum.itesym.group.id"));
		assertEquals("caelum-item",		props.get("caelum.itesym.source.topic"));
		assertEquals("1000",			props.get("caelum.itesym.poll.timeout"));
		assertEquals("15000",			props.get("caelum.itesym.shutdown.timeout"));
	}
	
	@Test
	public void testDefaults() throws Exception {
		assertEquals("app.itesym.properties", ItesymConfig.DEFAULT_CONFIG_FILE);
		assertEquals("app.itesym.properties", service.getDefaultConfigFile());
		
		verifyDefaultProperties(service.getProperties());
		
		Properties props = new Properties();
		assertTrue(service.loadFromResources(ItesymConfig.DEFAULT_CONFIG_FILE, props));
		
		verifyDefaultProperties(props);
	}
	
	@Test
	public void testGetKafkaProperties() {
		Properties props = service.getKafkaProperties();
		
		assertEquals(4, props.size());
		assertEquals("localhost:8082", props.getProperty("bootstrap.servers"));
		assertEquals("caelum-itesym", props.getProperty("group.id"));
		assertEquals("false", props.getProperty("enable.auto.commit"));
		assertEquals("read_committed", props.getProperty("isolation.level"));
	}
	
	@Test
	public void testGetSourceTopic() {
		assertEquals("caelum-item", service.getSourceTopic());
		
		service.getProperties().put("caelum.itesym.source.topic", "kukumber");
		assertEquals("kukumber", service.getSourceTopic());
		
		service.getProperties().put("caelum.itesym.source.topic", "bambazooka");
		assertEquals("bambazooka", service.getSourceTopic());
	}
	
	@Test
	public void testGetPollTimeout() {
		assertEquals(1000L, service.getPollTimeout());
		
		service.getProperties().put("caelum.itesym.poll.timeout", "2500");
		assertEquals(2500L, service.getPollTimeout());
		
		service.getProperties().put("caelum.itesym.poll.timeout", "1234");
		assertEquals(1234L, service.getPollTimeout());
	}
	
	@Test
	public void testGetShutdownTimeout() {
		assertEquals(15000L, service.getShutdownTimeout());
		
		service.getProperties().put("caelum.itesym.shutdown.timeout", "60000");
		assertEquals(60000L, service.getShutdownTimeout());
		
		service.getProperties().put("caelum.itesym.shutdown.timeout", "75000");
		assertEquals(75000L, service.getShutdownTimeout());
	}
	
	@Test
	public void testGetGroupId() {
		assertEquals("caelum-itesym", service.getGroupId());
		
		service.getProperties().put("caelum.itesym.group.id", "foobar");
		assertEquals("foobar", service.getGroupId());
	}

}
