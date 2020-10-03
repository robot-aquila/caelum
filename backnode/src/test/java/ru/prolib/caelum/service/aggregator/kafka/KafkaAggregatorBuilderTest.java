package ru.prolib.caelum.service.aggregator.kafka;

import static org.easymock.EasyMock.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

import org.easymock.Capture;
import org.easymock.IMocksControl;
import org.junit.Before;
import org.junit.Test;

import ru.prolib.caelum.lib.IService;
import ru.prolib.caelum.lib.Interval;
import ru.prolib.caelum.service.AggregatorType;
import ru.prolib.caelum.service.GeneralConfig;
import ru.prolib.caelum.service.IBuildingContext;
import ru.prolib.caelum.service.aggregator.IAggregator;
import ru.prolib.caelum.service.aggregator.kafka.KafkaAggregatorBuilder.Objects;
import ru.prolib.caelum.service.aggregator.kafka.utils.IRecoverableStreamsService;
import ru.prolib.caelum.service.aggregator.kafka.utils.RecoverableStreamsService;
import ru.prolib.caelum.service.aggregator.kafka.utils.RecoverableStreamsServiceStarter;
import ru.prolib.caelum.service.itemdb.kafka.utils.KafkaUtils;

public class KafkaAggregatorBuilderTest {
	IMocksControl control;
	KafkaUtils utils;
	KafkaAggregatorTopologyBuilder topologyBuilderMock;
	GeneralConfig gconfMock;
	KafkaAggregatorConfig config;
	KafkaStreamsRegistry registryMock;
	IBuildingContext contextMock;
	RecoverableStreamsService streamsServiceMock;
	Thread threadMock;
	IAggregator aggregatorMock;
	Lock mutexMock;
	Objects objects;
	KafkaAggregatorBuilder service, mockedService;

	@Before
	public void setUp() throws Exception {
		control = createStrictControl();
		utils = new KafkaUtils();
		topologyBuilderMock = control.createMock(KafkaAggregatorTopologyBuilder.class);
		gconfMock = control.createMock(GeneralConfig.class);
		config = new KafkaAggregatorConfig(Interval.M5, gconfMock);
		registryMock = control.createMock(KafkaStreamsRegistry.class);
		contextMock = control.createMock(IBuildingContext.class);
		streamsServiceMock = control.createMock(RecoverableStreamsService.class);
		aggregatorMock = control.createMock(IAggregator.class);
		mutexMock = control.createMock(Lock.class);
		objects = new Objects();
		service = new KafkaAggregatorBuilder(objects);
		mockedService = partialMockBuilder(KafkaAggregatorBuilder.class)
				.withConstructor(Objects.class)
				.withArgs(objects)
				.addMockedMethod("createStreamsService", KafkaAggregatorDescr.class)
				.addMockedMethod("createThread", String.class, Runnable.class)
				.addMockedMethod("createAggregator", KafkaAggregatorDescr.class, IRecoverableStreamsService.class)
				.createMock(control);
	}
	
	@Test
	public void testObjects_GetUtils_ShouldThrowsIfNotDefined() {
		IllegalStateException e = assertThrows(IllegalStateException.class, () -> objects.getUtils());
		assertEquals("Kafka utils was not defined", e.getMessage());
	}
	
	@Test
	public void testObjects_GetUtils() {
		assertSame(objects, objects.setUtils(utils));
		
		assertSame(utils, objects.getUtils());
	}
	
	@Test
	public void testObjects_GetTopologyBuilder_ShouldThrowsIfNotDefined() {
		IllegalStateException e = assertThrows(IllegalStateException.class, () -> objects.getTopologyBuilder());
		assertEquals("Topology builder was not defined", e.getMessage());
		
	}
	
	@Test
	public void testObjects_GetTopologyBuilder() {
		assertSame(objects, objects.setTopologyBuilder(topologyBuilderMock));
		
		assertSame(topologyBuilderMock, objects.getTopologyBuilder());
	}
	
	@Test
	public void testObjects_GetConfig_ShouldThrowsIfNotDefined() {
		IllegalStateException e = assertThrows(IllegalStateException.class, () -> objects.getConfig());
		assertEquals("Configuration was not defined", e.getMessage());
	}
	
	@Test
	public void testObjects_GetConfig() {
		assertSame(objects, objects.setConfig(config));
		
		assertSame(config, objects.getConfig());
	}
	
	@Test
	public void testObjects_GetStreamsRegistry_ShouldThrowsIfNotDefined() {
		IllegalStateException e = assertThrows(IllegalStateException.class, () -> objects.getStreamsRegistry());
		assertEquals("Streams registry was not defined", e.getMessage());
	}
	
	@Test
	public void testObjects_GetStreamsRegistry() {
		assertSame(objects, objects.setStreamsRegistry(registryMock));

		assertSame(registryMock, objects.getStreamsRegistry());
	}
	
	@Test
	public void testObjects_GetBuildingContext_ShouldThrowsIfNotDefined() {
		IllegalStateException e = assertThrows(IllegalStateException.class, () -> objects.getBuildingContext());
		assertEquals("Building context was not defined", e.getMessage());
	}
	
	@Test
	public void testObjects_GetBuildingContext() {
		assertSame(objects, objects.setBuildingContext(contextMock));
		
		assertSame(contextMock, objects.getBuildingContext());
	}
	
	@Test
	public void testObjects_GetCleanUpMutex_ShouldThrowsIfNotDefined() {
		IllegalStateException e = assertThrows(IllegalStateException.class, () -> objects.getCleanUpMutex());
		assertEquals("CleanUp mutex was not defined", e.getMessage());
	}
	
	@Test
	public void testObjects_GetCleanUpMutex() {
		assertSame(objects, objects.setCleanUpMutex(mutexMock));
		
		assertSame(mutexMock, objects.getCleanUpMutex());
	}
	
	@Test
	public void testWithUtils() {
		assertSame(service, service.withUtils(utils));
		
		assertSame(utils, objects.getUtils());
	}
	
	@Test
	public void testWithTopologyBuilder() {
		assertSame(service, service.withTopologyBuilder(topologyBuilderMock));
		
		assertSame(topologyBuilderMock, objects.getTopologyBuilder());
	}
	
	@Test
	public void testWithConfig() {
		assertSame(service, service.withConfig(config));

		assertSame(config, objects.getConfig());
	}
	
	@Test
	public void testWithStreamsRegistry() {
		assertSame(service, service.withStreamsRegistry(registryMock));

		assertSame(registryMock, objects.getStreamsRegistry());
	}
	
	@Test
	public void testWithBuildingContext() {
		assertSame(service, service.withBuildingContext(contextMock));

		assertSame(contextMock, objects.getBuildingContext());
	}
	
	@Test
	public void testWithCleanUpMutex() {
		assertSame(service, service.withCleanUpMutex(mutexMock));
		
		assertSame(mutexMock, objects.getCleanUpMutex());
	}
	
	@Test
	public void testCreateThread() throws Exception {
		CountDownLatch finished = new CountDownLatch(1);
		Runnable r = () -> {
			assertEquals("kumbacha", Thread.currentThread().getName());
			finished.countDown();
		};
		
		Thread t = service.createThread("kumbacha", r);
		
		assertNotNull(t);
		t.start();
		assertTrue(finished.await(1, TimeUnit.SECONDS));
	}
	
	@Test
	public void testCreateStreamsService() {
		expect(gconfMock.getMaxErrors()).andReturn(1000);
		objects.setTopologyBuilder(topologyBuilderMock)
			.setConfig(config)
			.setStreamsRegistry(registryMock)
			.setCleanUpMutex(mutexMock)
			.setUtils(utils);
		control.replay();
		KafkaAggregatorDescr d = new KafkaAggregatorDescr(AggregatorType.ITEM, Interval.M5, "source", "target", "store");

		
		RecoverableStreamsService actual = service.createStreamsService(d);
		
		control.verify();
		assertNotNull(actual);
		assertEquals(1000L, actual.getMaxErrors());
		KafkaStreamsController ctrl = (KafkaStreamsController) actual.getController();
		assertEquals(d, ctrl.getDescriptor());
		assertSame(topologyBuilderMock, ctrl.getTopologyBuilder());
		assertSame(config, ctrl.getConfig());
		assertSame(registryMock, ctrl.getStreamsRegistry());
		assertSame(mutexMock, ctrl.getCleanUpMutex());
		assertSame(utils, ctrl.getUtils());
		assertSame(registryMock, ctrl.getStreamsRegistry());
	}
	
	@Test
	public void testCreateAggregator() {
		objects.setConfig(config)
			.setUtils(utils)
			.setStreamsRegistry(registryMock);
		control.replay();
		KafkaAggregatorDescr d = new KafkaAggregatorDescr(AggregatorType.ITEM, Interval.M5, "source", "target", "store");
		
		IAggregator actual = service.createAggregator(d, streamsServiceMock);
		
		control.verify();
		assertNotNull(actual);
		assertThat(actual, is(instanceOf(KafkaAggregator.class)));
		KafkaAggregator x = (KafkaAggregator) actual;
		assertEquals(d, x.getDescriptor());
		assertSame(config, x.getConfig());
		assertSame(streamsServiceMock, x.getStreamsService());
		assertSame(utils,x.getUtils());
	}

	@Test
	public void testBuild() {
		expect(gconfMock.getItemsTopicName()).andStubReturn("source-data");
		expect(gconfMock.getAggregatorKafkaApplicationIdPrefix()).andStubReturn("MyApp-");
		expect(gconfMock.getAggregatorKafkaTargetTopicPrefix()).andStubReturn("target-");
		expect(gconfMock.getAggregatorKafkaStorePrefix()).andStubReturn("store-");
		expect(gconfMock.getDefaultTimeout()).andStubReturn(25000L);
		objects.setConfig(config).setBuildingContext(contextMock);
		KafkaAggregatorDescr d =
				new KafkaAggregatorDescr(AggregatorType.ITEM, Interval.M5, "source-data", "target-m5", "store-m5");
		Capture<IService> cap = newCapture();
		expect(mockedService.createStreamsService(d)).andReturn(streamsServiceMock);
		expect(mockedService.createThread("MyApp-m5-thread", streamsServiceMock)).andReturn(threadMock);
		expect(contextMock.registerService(capture(cap))).andReturn(contextMock);
		expect(mockedService.createAggregator(d, streamsServiceMock)).andReturn(aggregatorMock);
		control.replay();
		
		assertSame(aggregatorMock, mockedService.build());
		
		control.verify();
		assertThat(cap.getValue(), is(instanceOf(RecoverableStreamsServiceStarter.class)));
		RecoverableStreamsServiceStarter x = (RecoverableStreamsServiceStarter) cap.getValue();
		assertSame(threadMock, x.getThread());
		assertSame(streamsServiceMock, x.getStreamsService());
		assertEquals(25000L, x.getTimeout());
	}

}
