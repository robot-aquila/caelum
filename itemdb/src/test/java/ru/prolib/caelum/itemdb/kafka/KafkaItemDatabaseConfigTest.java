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
		assertEquals("localhost:8082",			props.get("caelum.itemdb.kafka.bootstrap.servers"));
		assertEquals("caelum-item",				props.get("caelum.itemdb.kafka.source.topic"));
		assertEquals("5000",					props.get("caelum.itemdb.list.items.limit"));
		assertEquals("caelum-itemdb-producer1",	props.get("caelum.itemdb.kafka.transactional.id"));
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
		assertEquals("earliest", props.get("auto.offset.reset"));
		assertEquals("false", props.get("enable.auto.commit"));
		assertEquals("read_committed", props.get("isolation.level"));
	}
	
	@Test
	public void testGetProducerKafkaProperties() {
		Properties props = service.getProducerKafkaProperties();
		
		assertEquals(2, props.size());
		assertEquals("localhost:8082", props.get("bootstrap.servers"));
		assertEquals("caelum-itemdb-producer1", props.get("transactional.id"));
	}
	
	@Test
	public void testGetAdminClientProperties() {
		Properties props = service.getAdminClientProperties();
		
		assertEquals(1, props.size());
		assertEquals("localhost:8082", props.get("bootstrap.servers"));
	}
	
	@Test
	public void testGetSourceTopic() {
		assertEquals("caelum-item", service.getSourceTopic());
	}

}
