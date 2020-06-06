package ru.prolib.caelum.test;

import static org.junit.Assert.*;

import java.util.Properties;

import org.junit.Before;
import org.junit.Test;

public class TupleConsumerConfigTest {
	TupleConsumerConfig service;

	@Before
	public void setUp() throws Exception {
		service = new TupleConsumerConfig();
	}
	
	void verifyDefaultProperties(Properties props) {
		assertEquals(3, props.size());
		assertEquals("localhost:8082",			props.get("caelum.tupleconsumer.bootstrap.servers"));
		assertEquals("caelum-tuple-consumer",	props.get("caelum.tupleconsumer.group.id"));
		assertEquals("caelum-tuple-m1",			props.get("caelum.tupleconsumer.source.topic"));
	}

	@Test
	public void testDefaults() throws Exception {
		assertEquals("app.tupleconsumer.properties", TupleConsumerConfig.DEFAULT_CONFIG_FILE);
		
		verifyDefaultProperties(service.getProperties());
		
		Properties props = new Properties();
		assertTrue(service.loadFromResources(TupleConsumerConfig.DEFAULT_CONFIG_FILE, props));
		verifyDefaultProperties(props);
	}

}
