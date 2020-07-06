package ru.prolib.caelum.aggregator;
import static org.junit.Assert.*;

import java.util.Properties;

import org.junit.Before;
import org.junit.Test;

public class ItemAggregatorConfigTest {
	ItemAggregatorConfig service;
	
	@Before
	public void setUp() throws Exception {
		service = new ItemAggregatorConfig();
	}

	void verifyDefaultProperties(Properties props) {
		assertEquals(6, props.size());
		assertEquals("caelum-item-aggregator-",	props.get("caelum.itemaggregator.pfx.application.id"));
		assertEquals("caelum-tuple-store-",		props.get("caelum.itemaggregator.pfx.aggregation.store"));
		assertEquals("caelum-tuple-",			props.get("caelum.itemaggregator.pfx.target.topic"));
		assertEquals("localhost:8082",			props.get("caelum.itemaggregator.bootstrap.servers"));
		assertEquals("caelum-item",				props.get("caelum.itemaggregator.source.topic"));
		assertEquals("M1",						props.get("caelum.itemaggregator.aggregation.period"));		
	}
	
	@Test
	public void testDefaults() throws Exception {
		assertEquals("app.itemaggregator.properties", ItemAggregatorConfig.DEFAULT_CONFIG_FILE);
		assertEquals("app.itemaggregator.properties", service.getDefaultConfigFile());
		
		verifyDefaultProperties(service.getProperties());
		
		Properties props = new Properties();
		assertTrue(service.loadFromResources(ItemAggregatorConfig.DEFAULT_CONFIG_FILE, props));
		verifyDefaultProperties(props);
	}

}
