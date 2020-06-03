package ru.prolib.caelum.aggregator;
import static org.junit.Assert.*;

import java.util.Properties;

import org.junit.Before;
import org.junit.Test;

import ru.prolib.caelum.aggregator.TradeAggregatorConfig;

public class TradeAggregatorConfigTest {
	TradeAggregatorConfig service;
	
	@Before
	public void setUp() throws Exception {
		service = new TradeAggregatorConfig();
	}

	void verifyDefaultProperties(Properties props) {
		assertEquals(6, props.size());
		assertEquals("caelum-trades-aggregator-",	props.get("caelum.tradeaggregator.pfx.application.id"));
		assertEquals("caelum-ohlcv-store-",			props.get("caelum.tradeaggregator.pfx.aggregation.store"));
		assertEquals("caelum-ohlcv-",				props.get("caelum.tradeaggregator.pfx.target.topic"));
		assertEquals("localhost:8082",				props.get("caelum.tradeaggregator.bootstrap.servers"));
		assertEquals("caelum-trades",				props.get("caelum.tradeaggregator.source.topic"));
		assertEquals("M1",							props.get("caelum.tradeaggregator.aggregation.period"));		
	}
	
	@Test
	public void testDefaults() throws Exception {
		assertEquals("app.tradeaggregator.properties", TradeAggregatorConfig.DEFAULT_CONFIG_FILE);
		
		verifyDefaultProperties(service.getProperties());
		
		Properties props = new Properties();
		assertTrue(service.loadFromResources(TradeAggregatorConfig.DEFAULT_CONFIG_FILE, props));
		verifyDefaultProperties(props);
	}

}
