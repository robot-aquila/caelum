package ru.prolib.caelum.service.aggregator.kafka;

import static org.junit.Assert.*;
import static ru.prolib.caelum.service.aggregator.kafka.KafkaAggregatorServiceBuilder.*;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.Arrays;
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

import ru.prolib.caelum.lib.HostInfo;
import ru.prolib.caelum.lib.IService;
import ru.prolib.caelum.lib.Interval;
import ru.prolib.caelum.lib.Intervals;
import ru.prolib.caelum.service.BuildingContext;
import ru.prolib.caelum.service.GeneralConfig;
import ru.prolib.caelum.service.GeneralConfigImpl;
import ru.prolib.caelum.service.IBuildingContext;
import ru.prolib.caelum.service.ServiceRegistry;
import ru.prolib.caelum.service.ServletRegistry;
import ru.prolib.caelum.service.aggregator.IAggregator;
import ru.prolib.caelum.service.aggregator.IAggregatorService;
import ru.prolib.caelum.service.itemdb.kafka.utils.KafkaCreateTopicService;
import ru.prolib.caelum.service.itemdb.kafka.utils.KafkaUtils;

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
	IBuildingContext contextMock;
	IService iserviceMock;
	GeneralConfigImpl gconf;
	IAggregator aggregatorMock1, aggregatorMock2, aggregatorMock3;
	KafkaStreamsRegistry sregistryMock;
	KafkaAggregatorTopologyBuilder topologyBuilderMock;
	KafkaUtils utilsMock;
	Lock mutexMock;
	KafkaAggregatorServiceBuilder service, mockedService;

	@Before
	public void setUp() throws Exception {
		hostInfo = new HostInfo("tutumbr", 2519);
		intervals = new Intervals();
		gconf = new GeneralConfigImpl(intervals);
		control = createStrictControl();
		builderMock = control.createMock(KafkaAggregatorBuilder.class);
		contextMock = control.createMock(IBuildingContext.class);
		aggregatorMock1 = control.createMock(IAggregator.class);
		aggregatorMock2 = control.createMock(IAggregator.class);
		aggregatorMock3 = control.createMock(IAggregator.class);
		sregistryMock = control.createMock(KafkaStreamsRegistry.class);
		topologyBuilderMock = control.createMock(KafkaAggregatorTopologyBuilder.class);
		utilsMock = control.createMock(KafkaUtils.class);
		mutexMock = control.createMock(Lock.class);
		iserviceMock = control.createMock(IService.class);
		service = new KafkaAggregatorServiceBuilder(builderMock, utilsMock);
		mockedService = partialMockBuilder(KafkaAggregatorServiceBuilder.class)
				.withConstructor(KafkaAggregatorBuilder.class, KafkaUtils.class)
				.withArgs(builderMock, utilsMock)
				.addMockedMethod("createStreamsRegistry", HostInfo.class, Intervals.class)
				.addMockedMethod("createLock")
				.addMockedMethod("createInitService", GeneralConfig.class)
				.createMock(control);
	}
	
	@Test
	public void testCtor2() {
		assertSame(builderMock, service.getKafkaAggregatorBuilder());
		assertSame(utilsMock, service.getKafkaUtils());
	}
	
	@Test
	public void testCtor0() {
		service = new KafkaAggregatorServiceBuilder();
		assertNotNull(service.getKafkaAggregatorBuilder());
		assertSame(KafkaUtils.getInstance(), service.getKafkaUtils());
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
	public void testCreateInitService() {
		gconf.setItemsTopicName("tora-tora")
			.setItemsTopicNumPartitions(24)
			.setItemsTopicReplicationFactor((short)2)
			.setItemsTopicRetentionTime(2226781)
			.setDefaultTimeout(2345);
		
		IService actual = service.createInitService(gconf);
		
		assertNotNull(actual);
		assertThat(actual, is(instanceOf(KafkaCreateTopicService.class)));
		IService expected = new KafkaCreateTopicService(utilsMock, gconf,
				new NewTopic("tora-tora", 24, (short)2).configs(toMap("retention.ms", "2226781")), 2345L);
		assertEquals(expected, actual);
	}
	
	@Test
	public void testBuild() throws Exception {
		// duplicates & whitespaces should be ignored
		gconf.setAggregatorInterval(" M1, M5, H1, M1, H1, M5, M5")
			.setMaxTuplesLimit(400)
			.setAggregatorKafkaForceParallelClear(null)
			.setAdvertisedHttpInfo("gap", 1345)
			.setDefaultTimeout(2345L);
		expect(utilsMock.isOsUnix()).andStubReturn(true);
		expect(contextMock.getConfig()).andStubReturn(gconf);
		expect(mockedService.createInitService(gconf)).andStubReturn(iserviceMock);
		expect(mockedService.createStreamsRegistry(new HostInfo("gap", 1345), intervals)).andStubReturn(sregistryMock);
		expect(mockedService.createLock()).andStubReturn(mutexMock);
		expect(contextMock.registerService(iserviceMock)).andReturn(contextMock);
		expect(builderMock.withBuildingContext(contextMock)).andReturn(builderMock);
		expect(builderMock.withStreamsRegistry(sregistryMock)).andReturn(builderMock);
		expect(builderMock.withTopologyBuilder(new KafkaAggregatorTopologyBuilder())).andReturn(builderMock);
		expect(builderMock.withCleanUpMutex(mutexMock)).andReturn(builderMock);
		expect(builderMock.withUtils(utilsMock)).andReturn(builderMock);
		expect(builderMock.withConfig(new KafkaAggregatorConfig(Interval.M1, gconf))).andReturn(builderMock);
		expect(builderMock.build()).andReturn(aggregatorMock1);
		expect(builderMock.withConfig(new KafkaAggregatorConfig(Interval.M5, gconf))).andReturn(builderMock);
		expect(builderMock.build()).andReturn(aggregatorMock2);
		expect(builderMock.withConfig(new KafkaAggregatorConfig(Interval.H1, gconf))).andReturn(builderMock);
		expect(builderMock.build()).andReturn(aggregatorMock3);
		control.replay();
		
		IAggregatorService actual = mockedService.build(contextMock);
		
		control.verify();
		assertThat(actual, is(instanceOf(KafkaAggregatorService.class)));
		KafkaAggregatorService x = (KafkaAggregatorService) actual;
		assertEquals(400, x.getMaxLimit());
		assertEquals(2345L, x.getTimeout());
		assertTrue(x.isClearAggregatorsInParallel());
		assertEquals(Arrays.asList(aggregatorMock1, aggregatorMock2, aggregatorMock3), x.getAggregatorList());
	}
	
	@Test
	public void testBuild_ShouldUseForceParallelClearFromConfig() throws Exception {
		gconf.setAggregatorInterval("M1")
			.setAggregatorKafkaForceParallelClear(false)
			.setAdvertisedHttpInfo("bak", 1345);
		expect(contextMock.getConfig()).andStubReturn(gconf);
		expect(mockedService.createInitService(gconf)).andStubReturn(iserviceMock);
		expect(mockedService.createStreamsRegistry(new HostInfo("bak", 1345), intervals)).andStubReturn(sregistryMock);
		expect(mockedService.createLock()).andStubReturn(mutexMock);
		expect(contextMock.registerService(iserviceMock)).andReturn(contextMock);
		expect(builderMock.withBuildingContext(contextMock)).andReturn(builderMock);
		expect(builderMock.withStreamsRegistry(sregistryMock)).andReturn(builderMock);
		expect(builderMock.withTopologyBuilder(new KafkaAggregatorTopologyBuilder())).andReturn(builderMock);
		expect(builderMock.withCleanUpMutex(mutexMock)).andReturn(builderMock);
		expect(builderMock.withUtils(utilsMock)).andReturn(builderMock);
		expect(builderMock.withConfig(new KafkaAggregatorConfig(Interval.M1, gconf))).andReturn(builderMock);
		expect(builderMock.build()).andReturn(aggregatorMock1);
		control.replay();
		
		IAggregatorService actual = mockedService.build(contextMock);
		
		control.verify();
		assertNotNull(actual);
		assertThat(actual, is(instanceOf(KafkaAggregatorService.class)));
		KafkaAggregatorService x = (KafkaAggregatorService) actual;
		assertFalse(x.isClearAggregatorsInParallel());
	}
	
	@Test
	public void testBuild_SmallIntegrationTest() throws Exception {
		service = new KafkaAggregatorServiceBuilder();
		
		assertNotNull(service.build(new BuildingContext(gconf, "foo", null, null,
				new ServiceRegistry(), new ServletRegistry())));
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
		assertTrue(service.equals(new KafkaAggregatorServiceBuilder(builderMock, utilsMock)));
		assertFalse(service.equals(null));
		assertFalse(service.equals(this));
		assertFalse(service.equals(new KafkaAggregatorServiceBuilder(new KafkaAggregatorBuilder(), utilsMock)));
	}

}
