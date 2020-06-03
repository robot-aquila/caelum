package ru.prolib.caelum.test;

import static org.junit.Assert.*;

import java.util.Properties;

import org.junit.Before;
import org.junit.Test;

public class OHLCVConsumerConfigTest {
	OHLCVConsumerConfig service;

	@Before
	public void setUp() throws Exception {
		service = new OHLCVConsumerConfig();
	}
	
	void verifyDefaultProperties(Properties props) {
		assertEquals(3, props.size());
		assertEquals("localhost:8082",			props.get("caelum.ohlcvconsumer.bootstrap.servers"));
		assertEquals("caelum-ohlcv-consumer",	props.get("caelum.ohlcvconsumer.group.id"));
		assertEquals("caelum-ohlcv-m1",			props.get("caelum.ohlcvconsumer.source.topic"));
	}

	@Test
	public void testDefaults() throws Exception {
		assertEquals("app.ohlcvconsumer.properties", OHLCVConsumerConfig.DEFAULT_CONFIG_FILE);
		
		verifyDefaultProperties(service.getProperties());
		
		Properties props = new Properties();
		assertTrue(service.loadFromResources(OHLCVConsumerConfig.DEFAULT_CONFIG_FILE, props));
		verifyDefaultProperties(props);
	}

}
