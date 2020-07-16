package ru.prolib.caelum.itemdb.kafka;

import static org.junit.Assert.*;

import java.util.Properties;

import org.junit.Before;
import org.junit.Test;

public class KafkaItemDatabaseConfigTest {
	KafkaItemDatabaseConfig service;

	@Before
	public void setUp() throws Exception {
		service = new KafkaItemDatabaseConfig();
	}
	
	void verifyDefaultProperties(Properties props) {
		assertEquals("localhost:8082",	props.get("caelum.itemdb.kafka.bootstrap.servers"));
		assertEquals("caelum-item-db",	props.get("caelum.itemdb.kafka.group.id"));
		assertEquals("caelum-item",		props.get("caelum.itemdb.kafka.source.topic"));
		assertEquals("5000",			props.get("caelum.itemdb.list.items.limit"));
		assertEquals("all",				props.get("caelum.itemdb.kafka.acks"));
	}
	
	@Test
	public void testDefaults() throws Exception {
		assertEquals("app.itemdb.properties", KafkaItemDatabaseConfig.DEFAULT_CONFIG_FILE);
		assertEquals("app.itemdb.properties", service.getDefaultConfigFile());
		
		verifyDefaultProperties(service.getProperties());
		
		Properties props = new Properties();
		assertTrue(service.loadFromResources(KafkaItemDatabaseConfig.DEFAULT_CONFIG_FILE, props));
		
		verifyDefaultProperties(props);
	}

	@Test
	public void testGetConsumerKafkaProperties() {
		Properties props = service.getConsumerKafkaProperties();
		
		assertEquals(4, props.size());
		assertEquals("localhost:8082", props.get("bootstrap.servers"));
		assertEquals("caelum-item-db", props.get("group.id"));
		assertEquals("earliest", props.get("auto.offset.reset"));
		assertEquals("false", props.get("enable.auto.commit"));
	}
	
	@Test
	public void testGetProducerKafkaProperties() {
		Properties props = service.getProducerKafkaProperties();
		
		assertEquals(2, props.size());
		assertEquals("localhost:8082", props.get("bootstrap.servers"));
		assertEquals("all", props.get("acks"));
	}
	
	@Test
	public void testGetSourceTopic() {
		assertEquals("caelum-item", service.getSourceTopic());
	}

}
