package ru.prolib.caelum.utils;

import static org.junit.Assert.*;

import java.util.Properties;

import org.junit.Before;
import org.junit.Test;

public class TradeGeneratorConfigTest {
	TradeGeneratorConfig service;

	@Before
	public void setUp() throws Exception {
		service = new TradeGeneratorConfig();
	}

	void verifyDefaultProperties(Properties props) {
		assertEquals(8, props.size());
		assertEquals("localhost:8082",	props.get("caelum.tradegenerator.bootstrap.servers"));
		assertEquals("caelum-item",		props.get("caelum.tradegenerator.target.topic"));
		assertEquals("459811",			props.get("caelum.tradegenerator.seed"));
		assertEquals("16",				props.get("caelum.tradegenerator.symbol.num"));
		assertEquals("4",				props.get("caelum.tradegenerator.symbol.chars"));
		assertEquals("",				props.get("caelum.tradegenerator.symbol.prefix"));
		assertEquals("",				props.get("caelum.tradegenerator.symbol.suffix"));
		assertEquals("120",				props.get("caelum.tradegenerator.trades.per.minute"));
	}
	
	@Test
	public void testDefaults() throws Exception {
		assertEquals("app.tradegenerator.properties", TradeGeneratorConfig.DEFAULT_CONFIG_FILE);
		
		verifyDefaultProperties(service.getProperties());
		
		Properties props = new Properties();
		assertTrue(service.loadFromResources(TradeGeneratorConfig.DEFAULT_CONFIG_FILE, props));
		verifyDefaultProperties(props);
	}

}
