package ru.prolib.caelum.service.aggregator;

import static org.junit.Assert.*;

import java.util.Properties;

import org.junit.Before;
import org.junit.Test;

import ru.prolib.caelum.service.aggregator.kafka.KafkaAggregatorServiceBuilder;

public class AggregatorConfigTest {
	AggregatorConfig service;

	@Before
	public void setUp() throws Exception {
		service = new AggregatorConfig();
	}
	
	void verifyDefaultProperties(Properties props) {
		assertEquals(KafkaAggregatorServiceBuilder.class.getName(), props.get("caelum.aggregator.builder"));
		assertEquals("M1,H1", props.get("caelum.aggregator.interval"));
		assertEquals("5000", props.get("caelum.aggregator.list.tuples.limit"));
	}

	@Test
	public void testDefaults() throws Exception {
		assertEquals("app.aggregator.properties", AggregatorConfig.DEFAULT_CONFIG_FILE);
		assertEquals("app.aggregator.properties", service.getDefaultConfigFile());
		
		verifyDefaultProperties(service.getProperties());
		
		Properties props = new Properties();
		assertTrue(service.loadFromResources(AggregatorConfig.DEFAULT_CONFIG_FILE, props));
		
		verifyDefaultProperties(props);
	}
	
	@Test
	public void testGetListTuplesLimit() {
		service.getProperties().put("caelum.aggregator.list.tuples.limit", "380");
		
		assertEquals(380, service.getListTuplesLimit());
	}

}
