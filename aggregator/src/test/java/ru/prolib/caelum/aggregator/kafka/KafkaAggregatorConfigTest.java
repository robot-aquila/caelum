package ru.prolib.caelum.aggregator.kafka;

import static org.junit.Assert.*;
import static org.easymock.EasyMock.*;

import java.time.Duration;
import java.util.Properties;

import org.apache.commons.lang3.SystemUtils;
import org.junit.Before;
import org.junit.Test;

import ru.prolib.caelum.core.Period;
import ru.prolib.caelum.core.Periods;
import ru.prolib.caelum.itemdb.kafka.KafkaItemSerdes;

public class KafkaAggregatorConfigTest {
	Periods periods;
	KafkaAggregatorConfig service, mockedService;
	
	@Before
	public void setUp() throws Exception {
		periods = new Periods();
		service = new KafkaAggregatorConfig(periods);
		mockedService = partialMockBuilder(KafkaAggregatorConfig.class)
				.withConstructor(Periods.class)
				.withArgs(periods)
				.addMockedMethod("isOsUnix")
				.createMock();
	}
	
	void verifyDefaultProperties(Properties props) {
		assertEquals("caelum-item-aggregator-",	props.get("caelum.aggregator.kafka.pfx.application.id"));
		assertEquals("caelum-tuple-store-",		props.get("caelum.aggregator.kafka.pfx.aggregation.store"));
		assertEquals("caelum-tuple-",			props.get("caelum.aggregator.kafka.pfx.target.topic"));
		assertEquals("localhost:8082",			props.get("caelum.aggregator.kafka.bootstrap.servers"));
		assertEquals("caelum-item",				props.get("caelum.aggregator.kafka.source.topic"));
		assertEquals("99",						props.get("caelum.aggregator.kafka.max.errors"));
		assertEquals("60000",					props.get("caelum.aggregator.kafka.default.timeout"));
		assertEquals("M1,H1",					props.get("caelum.aggregator.aggregation.period"));
		assertEquals("5000",					props.get("caelum.aggregator.list.tuples.limit"));
		assertEquals("",						props.get("caelum.aggregator.kafka.force.parallel.clear"));
	}
	
	@Test
	public void testGetters() {
		assertSame(periods, service.getPeriods());
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
	
	@Test
	public void testGetMaxErrors() {
		service.getProperties().put("caelum.aggregator.kafka.max.errors", "256");
		
		assertEquals(256, service.getMaxErrors());
	}
	
	@Test
	public void testGetDefaultTimeout() {
		service.getProperties().put("caelum.aggregator.kafka.default.timeout", "20000");
		
		assertEquals(20000L, service.getDefaultTimeout());
	}
	
	@Test
	public void testGetAdminClientProperties() {
		Properties props = service.getAdminClientProperties();
		
		assertEquals(1, props.size());
		assertEquals("localhost:8082", props.get("bootstrap.servers"));
	}

	@Test
	public void testIsOsUnix() {
		assertEquals(SystemUtils.IS_OS_UNIX, service.isOsUnix());
	}
	
	@Test
	public void testIsParallelClear() {
		expect(mockedService.isOsUnix())
			.andReturn(true).andReturn(false)
			.andReturn(true).andReturn(false)
			.andReturn(true).andReturn(false);
		replay(mockedService);
		
		assertTrue(mockedService.isParallelClear());
		assertFalse(mockedService.isParallelClear());
		
		mockedService.getProperties().put("caelum.aggregator.kafka.force.parallel.clear", "0");
		assertFalse(mockedService.isParallelClear());
		assertFalse(mockedService.isParallelClear());
		
		mockedService.getProperties().put("caelum.aggregator.kafka.force.parallel.clear", "1");
		assertTrue(mockedService.isParallelClear());
		assertTrue(mockedService.isParallelClear());
		
		verify(mockedService);
	}

}
