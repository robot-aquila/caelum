package ru.prolib.caelum.service.aggregator.kafka;

import static org.junit.Assert.*;
import static org.easymock.EasyMock.*;

import java.util.Properties;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.easymock.IMocksControl;
import org.junit.Before;
import org.junit.Test;

import ru.prolib.caelum.lib.HostInfo;
import ru.prolib.caelum.lib.Interval;
import ru.prolib.caelum.lib.kafka.KafkaItemSerdes;
import ru.prolib.caelum.service.GeneralConfig;

public class KafkaAggregatorConfigTest {
	IMocksControl control;
	GeneralConfig configMock;
	Interval interval;
	KafkaAggregatorConfig service, mockedService;
	
	@Before
	public void setUp() throws Exception {
		control = createStrictControl();
		configMock = control.createMock(GeneralConfig.class);
		interval = Interval.H1;
		service = new KafkaAggregatorConfig(interval, configMock);
	}
	
	@Test
	public void testGetters() {
		assertSame(interval, service.getInterval());
		assertSame(configMock, service.getConfig());
	}
	
	@Test
	public void testGetApplicationId() {
		expect(configMock.getAggregatorKafkaApplicationIdPrefix()).andStubReturn("zulu24-");
		control.replay();
		
		assertEquals("zulu24-h1", service.getApplicationId());
		
		control.verify();
	}
	
	@Test
	public void testGetStoreName() {
		expect(configMock.getAggregatorKafkaStorePrefix()).andStubReturn("kappa-store-");
		control.replay();
		
		assertEquals("kappa-store-h1", service.getStoreName());
		
		control.verify();
	}	
	
	@Test
	public void testGetStoreRetentionTime() {
		expect(configMock.getAggregatorKafkaStoreRetentionTime()).andReturn(2215992773L);
		control.replay();
		
		assertEquals(2215992773L, service.getStoreRetentionTime());
		
		control.verify();
	}
	
	@Test
	public void testGetAggregationInterval() {
		assertEquals(interval, service.getAggregationInterval());
		
		assertEquals(Interval.H1, service.getAggregationInterval());
	}
	
	@Test
	public void testGetTargetTopic() {
		expect(configMock.getAggregatorKafkaTargetTopicPrefix()).andReturn("gadboa-");
		control.replay();
		
		assertEquals("gadboa-h1", service.getTargetTopic());
		
		control.verify();
	}

	
	@Test
	public void testGetTargetTopic_ShouldReturnNullIfPrefixOfTargetTopicNotDefined() {
		expect(configMock.getAggregatorKafkaTargetTopicPrefix()).andReturn("");
		control.replay();
		
		assertNull(service.getTargetTopic());

		control.verify();
	}
	
	
	@Test
	public void testGetSourceTopic() {
		expect(configMock.getItemsTopicName()).andReturn("bumbazyaka");
		control.replay();
		
		assertEquals("bumbazyaka", service.getSourceTopic());
		
		control.verify();
	}

	@Test
	public void testGetKafkaStreamsProperties() {
		expect(configMock.getKafkaBootstrapServers()).andStubReturn("191.15.34.5:19987");
		expect(configMock.getAggregatorKafkaApplicationIdPrefix()).andStubReturn("omega-");
		expect(configMock.getHttpInfo()).andStubReturn(new HostInfo("172.15.26.19", 5002));
		expect(configMock.getAggregatorKafkaLingerMs()).andStubReturn(5L);
		expect(configMock.getKafkaStateDir()).andStubReturn("/tmp/kafka-streams");
		expect(configMock.getAggregatorKafkaNumStreamThreads()).andReturn(2);
		control.replay();
		
		Properties actual = service.getKafkaStreamsProperties();
		
		control.verify();
		Properties expected = new Properties();
		expected.put("application.id", "omega-h1");
		expected.put("bootstrap.servers", "191.15.34.5:19987");
		expected.put("default.key.serde", KafkaItemSerdes.keySerde().getClass());
		expected.put("default.value.serde", KafkaItemSerdes.itemSerde().getClass());
		expected.put("linger.ms", "5");
		expected.put("state.dir", "/tmp/kafka-streams");
		expected.put("application.server", "172.15.26.19:5002");
		expected.put("processing.guarantee", "exactly_once");
		expected.put("num.stream.threads", "2");
		assertEquals(expected, actual);
	}
	
	@Test
	public void testGetMaxErrors() {
		expect(configMock.getMaxErrors()).andReturn(256);
		control.replay();
		
		assertEquals(256, service.getMaxErrors());
		
		control.verify();
	}
	
	@Test
	public void testGetDefaultTimeout() {
		expect(configMock.getDefaultTimeout()).andReturn(20000L);
		control.replay();
		
		assertEquals(20000L, service.getDefaultTimeout());
		
		control.verify();
	}
	
	@Test
	public void testHashCode() {
		int expected = new HashCodeBuilder(752913, 47)
				.append(interval)
				.append(configMock)
				.build();
		
		assertEquals(expected, service.hashCode());
	}
	
	@SuppressWarnings("unlikely-arg-type")
	@Test
	public void testEquals() {
		GeneralConfig configMock2 = control.createMock(GeneralConfig.class);
		assertTrue(service.equals(service));
		assertTrue(service.equals(new KafkaAggregatorConfig(interval, configMock)));
		assertFalse(service.equals(null));
		assertFalse(service.equals(this));
		assertFalse(service.equals(new KafkaAggregatorConfig(Interval.M1, configMock)));
		assertFalse(service.equals(new KafkaAggregatorConfig(interval, configMock2)));
		assertFalse(service.equals(new KafkaAggregatorConfig(Interval.M1, configMock2)));
	}

}
