package ru.prolib.caelum.service.itemdb;

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
		assertEquals("ru.prolib.caelum.service.itemdb.kafka.KafkaItemDatabaseServiceBuilder",
				props.get("caelum.itemdb.builder"));
		assertEquals("5000", props.get("caelum.itemdb.list.items.limit"));
	}

	@Test
	public void testDefaults() throws Exception {
		assertEquals("app.itemdb.properties", ItemDatabaseConfig.DEFAULT_CONFIG_FILE);
		assertEquals("app.itemdb.properties", service.getDefaultConfigFile());
		
		verifyDefaultProperties(service.getProperties());
		
		Properties props = new Properties();
		assertTrue(service.loadFromResources(ItemDatabaseConfig.DEFAULT_CONFIG_FILE, props));
		
		verifyDefaultProperties(props);
	}

}
