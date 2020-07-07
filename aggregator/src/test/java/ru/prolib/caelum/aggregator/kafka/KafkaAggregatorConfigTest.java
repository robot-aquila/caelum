package ru.prolib.caelum.aggregator.kafka;
import static org.junit.Assert.*;

import java.time.Duration;
import java.util.Properties;

import org.junit.Before;
import org.junit.Test;

import ru.prolib.caelum.core.Period;
import ru.prolib.caelum.itemdb.kafka.KafkaItemSerdes;

public class KafkaAggregatorConfigTest {
	KafkaAggregatorConfig service;
	
	@Before
	public void setUp() throws Exception {
		service = new KafkaAggregatorConfig();
	}

	void verifyDefaultProperties(Properties props) {
		assertEquals("caelum-item-aggregator-",	props.get("caelum.aggregator.kafka.pfx.application.id"));
		assertEquals("caelum-tuple-store-",		props.get("caelum.aggregator.kafka.pfx.aggregation.store"));
		assertEquals("caelum-tuple-",			props.get("caelum.aggregator.kafka.pfx.target.topic"));
		assertEquals("localhost:8082",			props.get("caelum.aggregator.kafka.bootstrap.servers"));
		assertEquals("caelum-item",				props.get("caelum.aggregator.kafka.source.topic"));
		assertEquals("M1,H1",						props.get("caelum.aggregator.aggregation.period"));
		assertEquals("5000",					props.get("caelum.aggregator.list.tuples.limit"));
	}
	
	@Test
	public void testDefaults() throws Exception {
		assertEquals("app.aggregator.properties", KafkaAggregatorConfig.DEFAULT_CONFIG_FILE);
		assertEquals("app.aggregator.properties", service.getDefaultConfigFile());
		
		verifyDefaultProperties(service.getProperties());
		
		Properties props = new Properties();
		assertTrue(service.loadFromResources(KafkaAggregatorConfig.DEFAULT_CONFIG_FILE, props));
		verifyDefaultProperties(props);
	}
	
	@Test
	public void testGetApplicationId() {
		service.getProperties().put("caelum.aggregator.kafka.pfx.application.id", "zulu24-");
		service.getProperties().put("caelum.aggregator.aggregation.period", "M15");
		
		assertEquals("zulu24-m15", service.getApplicationId());
	}
	
	@Test
	public void testGetStoreName() {
		service.getProperties().put("caelum.aggregator.kafka.pfx.aggregation.store", "kappa-store-");
		service.getProperties().put("caelum.aggregator.aggregation.period", "H4");
		
		assertEquals("kappa-store-h4", service.getStoreName());
	}
	
	@Test
	public void testGetAggregationPeriodCode() {
		service.getProperties().put("caelum.aggregator.aggregation.period", "H12");
		
		assertEquals("H12", service.getAggregationPeriodCode());
	}
	
	@Test
	public void testGetAggregationPeriod() {
		service.getProperties().put("caelum.aggregator.aggregation.period", "M30");
		
		assertEquals(Period.M30, service.getAggregationPeriod());
	}
	
	@Test
	public void testGetAggregationPeriodDuration() {
		service.getProperties().put("caelum.aggregator.aggregation.period", "M15");
		
		assertEquals(Duration.ofMinutes(15), service.getAggregationPeriodDuration());
	}
	
	@Test
	public void testGetTargetTopic() {
		service.getProperties().put("caelum.aggregator.kafka.pfx.target.topic", "");
		service.getProperties().put("caelum.aggregator.aggregation.period", "M5");
		
		assertNull(service.getTargetTopic());
		
		service.getProperties().put("caelum.aggregator.kafka.pfx.target.topic", "gadboa-");
		service.getProperties().put("caelum.aggregator.aggregation.period", "M5");
		
		assertEquals("gadboa-m5", service.getTargetTopic());
	}
	
	@Test
	public void testGetSourceTopic() {
		service.getProperties().put("caelum.aggregator.kafka.source.topic", "bumbazyaka");
		
		assertEquals("bumbazyaka", service.getSourceTopic());
	}
	
	@Test
	public void testGetKafkaProperties() {
		service.getProperties().put("caelum.aggregator.kafka.bootstrap.servers", "191.15.34.5:19987");
		service.getProperties().put("caelum.aggregator.kafka.pfx.application.id", "omega-");
		service.getProperties().put("caelum.aggregator.aggregation.period", "M5");
		
		Properties props = service.getKafkaProperties();
		assertNotSame(service.getProperties(), props);
		assertNotSame(props, service.getKafkaProperties()); // it's a new properties every call
		
		assertEquals(4, props.size());
		assertEquals("omega-m5", props.get("application.id"));
		assertEquals("191.15.34.5:19987", props.get("bootstrap.servers"));
		assertEquals(KafkaItemSerdes.keySerde().getClass(), props.get("default.key.serde"));
		assertEquals(KafkaItemSerdes.itemSerde().getClass(), props.get("default.value.serde"));
	}

}
