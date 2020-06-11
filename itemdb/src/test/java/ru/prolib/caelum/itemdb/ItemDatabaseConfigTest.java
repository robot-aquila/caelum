package ru.prolib.caelum.itemdb;

import static org.junit.Assert.*;

import java.util.Properties;

import org.junit.Before;
import org.junit.Test;

public class ItemDatabaseConfigTest {
	ItemDatabaseConfig service;

	@Before
	public void setUp() throws Exception {
		service = new ItemDatabaseConfig();
	}
	
	void verifyDefaultProperties(Properties props) {
		assertEquals(4, props.size());
		assertEquals("localhost:8082", props.get("caelum.itemdb.bootstrap.servers"));
		assertEquals("caelum-item-db", props.get("caelum.itemdb.group.id"));
		assertEquals("caelum-item", props.get("caelum.itemdb.source.topic"));
		assertEquals("5000", props.get("caelum.itemdb.limit"));
	}
	
	@Test
	public void testDefaults() throws Exception {
		assertEquals("app.itemdb.properties", ItemDatabaseConfig.DEFAULT_CONFIG_FILE);
		
		verifyDefaultProperties(service.getProperties());
		
		Properties props = new Properties();
		assertTrue(service.loadFromResources(ItemDatabaseConfig.DEFAULT_CONFIG_FILE, props));
		verifyDefaultProperties(props);
	}

	@Test
	public void testKafkaProperties() {
		Properties props = service.getKafkaProperties();
		
		assertEquals(4, props.size());
		assertEquals("localhost:8082", props.get("bootstrap.servers"));
		assertEquals("caelum-item-db", props.get("group.id"));
		assertEquals("earliest", props.get("auto.offset.reset"));
		assertEquals("false", props.get("enable.auto.commit"));
	}
	
	@Test
	public void testGetSourceTopic() {
		assertEquals("caelum-item", service.getSourceTopic());
	}

}
