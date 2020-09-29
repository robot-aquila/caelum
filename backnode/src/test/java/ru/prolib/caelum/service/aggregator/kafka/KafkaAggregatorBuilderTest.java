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

import ru.prolib.caelum.lib.CompositeService;
import ru.prolib.caelum.lib.IService;
import ru.prolib.caelum.lib.Interval;
import ru.prolib.caelum.lib.Intervals;
import ru.prolib.caelum.service.AggregatorType;
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
	KafkaAggregatorConfig config;
	KafkaStreamsRegistry registryMock;
	CompositeService servicesMock;
	RecoverableStreamsService streamsServiceMock;
	Thread threadMock;
	IAggregator aggregatorMock;
	Lock mutexMock;
	Objects objects;
	KafkaAggregatorBuilder service, mockedService;
	KafkaAggregatorDescr descr;

	@Before
	public void setUp() throws Exception {
		control = createStrictControl();
		utils = new KafkaUtils();
		topologyBuilderMock = control.createMock(KafkaAggregatorTopologyBuilder.class);
		config = new KafkaAggregatorConfig(new Intervals());
		registryMock = control.createMock(KafkaStreamsRegistry.class);
		servicesMock = control.createMock(CompositeService.class);
		streamsServiceMock = control.createMock(RecoverableStreamsService.class);
		aggregatorMock = control.createMock(IAggregator.class);
		mutexMock = control.createMock(Lock.class);
		objects = new Objects();
		service = new KafkaAggregatorBuilder(objects);
		descr = new KafkaAggregatorDescr(AggregatorType.ITEM, Interval.M6, "source", "taerget", "store");
		mockedService = partialMockBuilder(KafkaAggregatorBuilder.class)
				.withConstructor(Objects.class)
				.withArgs(objects)
				.addMockedMethod("createStreamsService", KafkaAggregatorDescr.class)
				.addMockedMethod("createThread", String.class, Runnable.class)
				.addMockedMethod("createAggregator", KafkaAggregatorDescr.class, IRecoverableStreamsService.class)
				.createMock();
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
	public void testObjects_GetServices_ShouldThrowsIfNotDefined() {
		IllegalStateException e = assertThrows(IllegalStateException.class, () -> objects.getServices());
		assertEquals("Services was not defined", e.getMessage());
	}
	
	@Test
	public void testObjects_GetServices() {
		assertSame(objects, objects.setServices(servicesMock));
		
		assertSame(servicesMock, objects.getServices());
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
	public void testWithServices() {
		assertSame(service, service.withServices(servicesMock));

		assertSame(servicesMock, objects.getServices());
	}
	
	@Test
	public void testWithCleanUoMutex() {
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
		objects.setTopologyBuilder(topologyBuilderMock)
			.setConfig(config)
			.setStreamsRegistry(registryMock)
			.setCleanUpMutex(mutexMock)
			.setUtils(utils);
		config.getProperties().put("caelum.aggregator.kafka.max.errors", "1000");
		
		RecoverableStreamsService actual = service.createStreamsService(descr);
		
		assertNotNull(actual);
		assertEquals(1000L, actual.getMaxErrors());
		KafkaStreamsController ctrl = (KafkaStreamsController) actual.getController();
		assertEquals(descr, ctrl.getDescriptor());
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
		
		IAggregator actual = service.createAggregator(descr, streamsServiceMock);
		
		assertNotNull(actual);
		assertThat(actual, is(instanceOf(KafkaAggregator.class)));
		KafkaAggregator x = (KafkaAggregator) actual;
		assertEquals(descr, x.getDescriptor());
		assertSame(config, x.getConfig());
		assertSame(streamsServiceMock, x.getStreamsService());
		assertSame(utils,x.getUtils());
	}

	@Test
	public void testBuild() {
		config.getProperties().put("caelum.aggregator.interval", "M5");
		config.getProperties().put("caelum.aggregator.kafka.source.topic", "source-data");
		config.getProperties().put("caelum.aggregator.kafka.pfx.application.id", "MyApp-");
		config.getProperties().put("caelum.aggregator.kafka.pfx.target.topic", "target-");
		config.getProperties().put("caelum.aggregator.kafka.pfx.aggregation.store", "store-");
		config.getProperties().put("caelum.aggregator.kafka.default.timeout", "25000");
		objects.setConfig(config).setServices(servicesMock);
		KafkaAggregatorDescr expected_descr =
				new KafkaAggregatorDescr(AggregatorType.ITEM, Interval.M5, "source-data", "target-m5", "store-m5");
		Capture<IService> cap = newCapture();
		expect(mockedService.createStreamsService(expected_descr)).andReturn(streamsServiceMock);
		expect(mockedService.createThread("MyApp-m5-thread", streamsServiceMock)).andReturn(threadMock);
		expect(servicesMock.register(capture(cap))).andReturn(servicesMock);
		expect(mockedService.createAggregator(expected_descr, streamsServiceMock)).andReturn(aggregatorMock);
		control.replay();
		replay(mockedService);
		
		assertSame(aggregatorMock, mockedService.build());
		
		verify(mockedService);
		control.verify();
		assertThat(cap.getValue(), is(instanceOf(RecoverableStreamsServiceStarter.class)));
		RecoverableStreamsServiceStarter x = (RecoverableStreamsServiceStarter) cap.getValue();
		assertSame(threadMock, x.getThread());
		assertSame(streamsServiceMock, x.getStreamsService());
		assertEquals(25000L, x.getTimeout());
	}

}