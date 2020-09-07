package ru.prolib.caelum.aggregator.kafka;

import static org.junit.Assert.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static ru.prolib.caelum.aggregator.kafka.KafkaAggregatorServiceBuilder.*;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.easymock.EasyMock.*;
import static org.hamcrest.Matchers.*;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.log4j.BasicConfigurator;
import org.easymock.IMocksControl;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import ru.prolib.caelum.aggregator.IAggregator;
import ru.prolib.caelum.aggregator.IAggregatorService;
import ru.prolib.caelum.core.CompositeService;
import ru.prolib.caelum.core.HostInfo;
import ru.prolib.caelum.core.Intervals;
import ru.prolib.caelum.itemdb.kafka.utils.KafkaCreateTopicService;
import ru.prolib.caelum.itemdb.kafka.utils.KafkaUtils;

public class KafkaAggregatorServiceBuilderTest {
	
	@BeforeClass
	public static void setUpBeforeClass() {
		BasicConfigurator.resetConfiguration();
		BasicConfigurator.configure();
	}
	
	IMocksControl control;
	HostInfo hostInfo;
	Intervals intervals;
	KafkaAggregatorBuilder builderMock;
	CompositeService servicesMock;
	KafkaAggregatorConfig mockedConfig, config1, config2, config3;
	IAggregator aggregatorMock1, aggregatorMock2, aggregatorMock3;
	KafkaStreamsRegistry streamsRegistryMock;
	KafkaAggregatorTopologyBuilder topologyBuilderMock;
	KafkaUtils utilsMock;
	Lock mutexMock;
	KafkaAggregatorServiceBuilder service, mockedService;

	@Before
	public void setUp() throws Exception {
		hostInfo = new HostInfo("tutumbr", 2519);
		intervals = new Intervals();
		control = createStrictControl();
		builderMock = control.createMock(KafkaAggregatorBuilder.class);
		servicesMock = control.createMock(CompositeService.class);
		mockedConfig = partialMockBuilder(KafkaAggregatorConfig.class)
				.withConstructor(Intervals.class)
				.withArgs(intervals)
				.addMockedMethod("load", String.class, String.class)
				.createMock();
		config1 = new KafkaAggregatorConfig(intervals);
		config2 = new KafkaAggregatorConfig(intervals);
		config3 = new KafkaAggregatorConfig(intervals);
		aggregatorMock1 = control.createMock(IAggregator.class);
		aggregatorMock2 = control.createMock(IAggregator.class);
		aggregatorMock3 = control.createMock(IAggregator.class);
		streamsRegistryMock = control.createMock(KafkaStreamsRegistry.class);
		topologyBuilderMock = control.createMock(KafkaAggregatorTopologyBuilder.class);
		utilsMock = control.createMock(KafkaUtils.class);
		mutexMock = control.createMock(Lock.class);
		service = new KafkaAggregatorServiceBuilder(builderMock);
		mockedService = partialMockBuilder(KafkaAggregatorServiceBuilder.class)
				.withConstructor(KafkaAggregatorBuilder.class)
				.addMockedMethod("createIntervals")
				.addMockedMethod("createUtils")
				.addMockedMethod("createConfig", Intervals.class)
				.addMockedMethod("createStreamsRegistry", HostInfo.class, Intervals.class)
				.addMockedMethod("createTopologyBuilder")
				.addMockedMethod("createLock")
				.withArgs(builderMock)
				.createMock();
	}
	
	@Test
	public void testCreateIntervals() {
		Intervals actual = service.createIntervals();
		
		assertNotNull(actual);
	}
	
	@Test
	public void testCreateUtils() {
		KafkaUtils actual = service.createUtils();
		
		assertSame(KafkaUtils.getInstance(), actual);
	}
	
	@Test
	public void testCreateConfig() {
		KafkaAggregatorConfig actual = service.createConfig(intervals);
		
		assertNotNull(actual);
		assertSame(intervals, actual.getIntervals());
	}
	
	@Test
	public void testCreateTopologyBuilder() {
		KafkaAggregatorTopologyBuilder actual = service.createTopologyBuilder();
		
		assertNotNull(actual);
	}
	
	@Test
	public void testCreateStreamsRegistry() {
		KafkaStreamsRegistry actual = service.createStreamsRegistry(hostInfo, intervals);
		
		assertNotNull(actual);
		assertEquals(hostInfo, actual.getHostInfo());
		assertSame(intervals, actual.getIntervals());
	}
	
	@Test
	public void testCreateLock() {
		Lock actual = service.createLock();
		
		assertNotNull(actual);
		assertThat(actual, is(instanceOf(ReentrantLock.class)));
	}
	
	@Test
	public void testBuild() throws Exception {
		mockedConfig.load("kappa.props", "beta.props");
		// duplicates should be ignored
		Properties props = mockedConfig.getProperties();
		props.put("caelum.aggregator.interval", " M1, M5, H1, M1, H1, M5, M5");
		props.put("caelum.aggregator.list.tuples.limit", "400");
		props.put("caelum.aggregator.kafka.force.parallel.clear", "1");
		props.put("caelum.aggregator.kafka.default.timeout", "2345");
		props.put("caelum.aggregator.kafka.application.server", "gap:1345");
		props.put("caelum.aggregator.kafka.source.topic", "tora-tora");
		props.put("caelum.aggregator.kafka.source.topic.num.partitions", "24");
		props.put("caelum.aggregator.kafka.source.topic.replication.factor", "2");
		props.put("caelum.aggregator.kafka.source.topic.retention.time", "2226781");
		expect(mockedService.createIntervals()).andReturn(intervals);
		expect(mockedService.createConfig(intervals)).andReturn(mockedConfig);
		expect(mockedService.createStreamsRegistry(new HostInfo("gap", 1345), intervals)).andReturn(streamsRegistryMock);
		expect(mockedService.createTopologyBuilder()).andReturn(topologyBuilderMock);
		expect(mockedService.createLock()).andReturn(mutexMock);
		expect(mockedService.createUtils()).andReturn(utilsMock);
		expect(servicesMock.register(new KafkaCreateTopicService(utilsMock,
				mockedConfig.getAdminClientProperties(),
				new NewTopic("tora-tora", 24, (short)2).configs(toMap("retention.ms", "2226781")),
				2345L))).andReturn(servicesMock);
		expect(builderMock.withServices(servicesMock)).andReturn(builderMock);
		expect(builderMock.withStreamsRegistry(streamsRegistryMock)).andReturn(builderMock);
		expect(builderMock.withTopologyBuilder(topologyBuilderMock)).andReturn(builderMock);
		expect(builderMock.withCleanUpMutex(mutexMock)).andReturn(builderMock);
		expect(builderMock.withUtils(utilsMock)).andReturn(builderMock);
		// create M1 aggregator
		expect(mockedService.createConfig(intervals)).andReturn(config1);
		expect(builderMock.withConfig(config1)).andReturn(builderMock);
		expect(builderMock.build()).andReturn(aggregatorMock1);
		// create M5 aggregator
		expect(mockedService.createConfig(intervals)).andReturn(config2);
		expect(builderMock.withConfig(config2)).andReturn(builderMock);
		expect(builderMock.build()).andReturn(aggregatorMock2);
		// create H1 aggregator
		expect(mockedService.createConfig(intervals)).andReturn(config3);
		expect(builderMock.withConfig(config3)).andReturn(builderMock);
		expect(builderMock.build()).andReturn(aggregatorMock3);
		control.replay();
		replay(mockedService);
		replay(mockedConfig);
		
		IAggregatorService actual = mockedService.build("kappa.props", "beta.props", servicesMock);
		
		verify(mockedConfig);
		verify(mockedService);
		control.verify();
		assertThat(actual, is(instanceOf(KafkaAggregatorService.class)));
		KafkaAggregatorService x = (KafkaAggregatorService) actual;
		assertEquals(Arrays.asList(aggregatorMock1, aggregatorMock2, aggregatorMock3), x.getAggregatorList());
		assertEquals(400, x.getMaxLimit()); // caelum.aggregator.list.tuples.limit
		assertEquals(2345L, x.getTimeout()); // caelum.aggregator.kafka.default.timeout
		assertTrue(x.isClearAggregatorsInParallel());
		Properties expected_props = new Properties();
		expected_props.putAll(mockedConfig.getProperties());
		expected_props.put("caelum.aggregator.interval", "M1");
		assertEquals(expected_props, config1.getProperties());
		expected_props.put("caelum.aggregator.interval", "M5");
		assertEquals(expected_props, config2.getProperties());
		expected_props.put("caelum.aggregator.interval", "H1");
		assertEquals(expected_props, config3.getProperties());
	}
	
	@Test
	public void testBuild_ShouldUseForceParallelClearFromConfig() throws Exception {
		mockedService = partialMockBuilder(KafkaAggregatorServiceBuilder.class)
				.withConstructor(KafkaAggregatorBuilder.class)
				.addMockedMethod("createConfig", Intervals.class)
				.withArgs(new KafkaAggregatorBuilder())
				.createMock();
		expect(mockedService.createConfig(anyObject())).andReturn(mockedConfig);
		expect(servicesMock.register(anyObject())).andReturn(servicesMock);
		mockedConfig.load("bubba.hut", "jubba.hut");
		mockedConfig.getProperties().put("caelum.aggregator.interval", "");
		mockedConfig.getProperties().put("caelum.aggregator.kafka.force.parallel.clear", "0");
		control.replay();
		replay(mockedService);
		replay(mockedConfig);
		
		IAggregatorService actual = mockedService.build("bubba.hut", "jubba.hut", servicesMock);
		
		verify(mockedConfig);
		verify(mockedService);
		control.verify();
		assertNotNull(actual);
		assertThat(actual, is(instanceOf(KafkaAggregatorService.class)));
		KafkaAggregatorService x = (KafkaAggregatorService) actual;
		assertFalse(x.isClearAggregatorsInParallel());
	}
	
	@Test
	public void testBuild_SmallIntegrationTest() throws Exception {
		service = new KafkaAggregatorServiceBuilder();
		CompositeService services = new CompositeService();
		
		IAggregatorService actual = service.build(KafkaAggregatorConfig.DEFAULT_CONFIG_FILE, null, services);
		
		assertNotNull(actual);
	}
	
	@Test
	public void testHashCode() {
		int expected = new HashCodeBuilder(59710737, 15)
				.append(builderMock)
				.build();
		
		assertEquals(expected, service.hashCode());
	}
	
	@SuppressWarnings("unlikely-arg-type")
	@Test
	public void testEquals() {
		assertTrue(service.equals(service));
		assertTrue(service.equals(new KafkaAggregatorServiceBuilder(builderMock)));
		assertFalse(service.equals(null));
		assertFalse(service.equals(this));
		assertFalse(service.equals(new KafkaAggregatorServiceBuilder(new KafkaAggregatorBuilder())));
	}

}
