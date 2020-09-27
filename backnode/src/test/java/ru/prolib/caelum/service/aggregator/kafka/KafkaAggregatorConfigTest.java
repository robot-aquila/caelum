package ru.prolib.caelum.service.aggregator.kafka;

import static org.junit.Assert.*;
import static org.easymock.EasyMock.*;

import java.time.Duration;
import java.util.Properties;

import org.apache.commons.lang3.SystemUtils;
import org.junit.Before;
import org.junit.Test;

import ru.prolib.caelum.lib.HostInfo;
import ru.prolib.caelum.lib.Interval;
import ru.prolib.caelum.lib.Intervals;
import ru.prolib.caelum.lib.kafka.KafkaItemSerdes;

public class KafkaAggregatorConfigTest {
	Intervals intervals;
	KafkaAggregatorConfig service, mockedService;
	
	@Before
	public void setUp() throws Exception {
		intervals = new Intervals();
		service = new KafkaAggregatorConfig(intervals);
		mockedService = partialMockBuilder(KafkaAggregatorConfig.class)
				.withConstructor(Intervals.class)
				.withArgs(intervals)
				.addMockedMethod("isOsUnix")
				.createMock();
	}
	
	void verifyDefaultProperties(Properties props) {
		assertEquals("M1,H1",					props.get("caelum.aggregator.interval"));
		assertEquals("5000",					props.get("caelum.aggregator.list.tuples.limit"));
		assertEquals("caelum-item-aggregator-",	props.get("caelum.aggregator.kafka.pfx.application.id"));
		assertEquals("caelum-tuple-store-",		props.get("caelum.aggregator.kafka.pfx.aggregation.store"));
		assertEquals("caelum-tuple-",			props.get("caelum.aggregator.kafka.pfx.target.topic"));
		assertEquals("localhost:8082",			props.get("caelum.aggregator.kafka.bootstrap.servers"));
		assertEquals("caelum-item",				props.get("caelum.aggregator.kafka.source.topic"));
		assertEquals("1",						props.get("caelum.aggregator.kafka.source.topic.replication.factor"));
		assertEquals("8",						props.get("caelum.aggregator.kafka.source.topic.num.partitions"));
		assertEquals("31536000000000",			props.get("caelum.aggregator.kafka.source.topic.retention.time"));
		assertEquals("99",						props.get("caelum.aggregator.kafka.max.errors"));
		assertEquals("60000",					props.get("caelum.aggregator.kafka.default.timeout"));
		assertEquals("",						props.get("caelum.aggregator.kafka.force.parallel.clear"));
		assertEquals("5",						props.get("caelum.aggregator.kafka.linger.ms"));
		assertEquals("/tmp/kafka-streams",		props.get("caelum.aggregator.kafka.state.dir"));
		assertEquals("2",						props.get("caelum.aggregator.kafka.num.stream.threads"));
		assertEquals("31536000000000",			props.get("caelum.aggregator.kafka.store.retention.time"));
		assertEquals("localhost:9698",			props.get("caelum.aggregator.kafka.application.server"));
	}
	
	@Test
	public void testGetters() {
		assertSame(intervals, service.getIntervals());
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
		service.getProperties().put("caelum.aggregator.interval", "M15");
		
		assertEquals("zulu24-m15", service.getApplicationId());
	}
	
	@Test
	public void testGetStoreName() {
		service.getProperties().put("caelum.aggregator.kafka.pfx.aggregation.store", "kappa-store-");
		service.getProperties().put("caelum.aggregator.interval", "H4");
		
		assertEquals("kappa-store-h4", service.getStoreName());
	}
	
	@Test
	public void testGetStoreRetentionTime() {
		service.getProperties().put("caelum.aggregator.kafka.store.retention.time", "2215992773");
		
		assertEquals(2215992773L, service.getStoreRetentionTime());
	}
	
	@Test
	public void testGetAggregationIntervalCode() {
		service.getProperties().put("caelum.aggregator.interval", "H12");
		
		assertEquals("H12", service.getAggregationIntervalCode());
	}
	
	@Test
	public void testGetAggregationInterval() {
		service.getProperties().put("caelum.aggregator.interval", "M30");
		
		assertEquals(Interval.M30, service.getAggregationInterval());
	}
	
	@Test
	public void testGetAggregationIntervalDuration() {
		service.getProperties().put("caelum.aggregator.interval", "M15");
		
		assertEquals(Duration.ofMinutes(15), service.getAggregationIntervalDuration());
	}
	
	@Test
	public void testGetTargetTopic() {
		service.getProperties().put("caelum.aggregator.kafka.pfx.target.topic", "");
		service.getProperties().put("caelum.aggregator.interval", "M5");
		
		assertNull(service.getTargetTopic());
		
		service.getProperties().put("caelum.aggregator.kafka.pfx.target.topic", "gadboa-");
		service.getProperties().put("caelum.aggregator.interval", "M5");
		
		assertEquals("gadboa-m5", service.getTargetTopic());
	}
	
	@Test
	public void testGetSourceTopic() {
		service.getProperties().put("caelum.aggregator.kafka.source.topic", "bumbazyaka");
		
		assertEquals("bumbazyaka", service.getSourceTopic());
	}
	
	@Test
	public void testGetSourceTopicNumPartitions() {
		service.getProperties().put("caelum.aggregator.kafka.source.topic.num.partitions", "24");
		
		assertEquals(24, service.getSourceTopicNumPartitions());
	}
	
	@Test
	public void testGetSourceTopicReplicationFactor() {
		service.getProperties().put("caelum.aggregator.kafka.source.topic.replication.factor", "3");
		
		short expected = 3;
		assertEquals(expected, service.getSourceTopicReplicationFactor());
	}
	
	@Test
	public void testGetSourceTopicRetentionTime() {
		service.getProperties().put("caelum.aggregator.kafka.source.topic.retention.time", "1234567890");
		
		long expected = 1234567890L;
		assertEquals(expected, service.getSourceTopicRetentionTime());
	}

	@Test
	public void testGetKafkaProperties() {
		service.getProperties().put("caelum.aggregator.kafka.bootstrap.servers", "191.15.34.5:19987");
		service.getProperties().put("caelum.aggregator.kafka.pfx.application.id", "omega-");
		service.getProperties().put("caelum.aggregator.interval", "M5");
		service.getProperties().put("caelum.aggregator.kafka.application.server", "172.15.26.19:5002");
		
		
		Properties props = service.getKafkaProperties();
		assertNotSame(service.getProperties(), props);
		assertNotSame(props, service.getKafkaProperties()); // it's a new properties every call
		
		assertEquals(9, props.size());
		assertEquals("omega-m5", props.get("application.id"));
		assertEquals("191.15.34.5:19987", props.get("bootstrap.servers"));
		assertEquals(KafkaItemSerdes.keySerde().getClass(), props.get("default.key.serde"));
		assertEquals(KafkaItemSerdes.itemSerde().getClass(), props.get("default.value.serde"));
		assertEquals("5", props.get("linger.ms"));
		assertEquals("/tmp/kafka-streams", props.get("state.dir"));
		assertEquals("2", props.get("num.stream.threads"));
		assertEquals("172.15.26.19:5002", props.get("application.server"));
		// forced settings
		assertEquals("exactly_once", props.get("processing.guarantee"));
		//assertEquals("500", props.get("acceptable.recovery.lag"));
		//assertEquals("500", props.get("max.poll.records"));
		//assertEquals("60000", props.get("transaction.timeout.ms"));
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
	
	@Test
	public void testGetApplicationServer() {
		service.getProperties().put("caelum.aggregator.kafka.application.server", "bambata:250");
		
		assertEquals(new HostInfo("bambata", 250), service.getApplicationServer());
	}
	
	@Test
	public void testGetApplicationServer_ShouldUseDefaultPortIfNotSpecified() {
		service.getProperties().put("caelum.aggregator.kafka.application.server", "chukaban");
		
		assertEquals(new HostInfo("chukaban", 9698), service.getApplicationServer());
	}

}
