package ru.prolib.caelum.aggregator.kafka;

import static org.junit.Assert.*;
import static org.hamcrest.Matchers.*;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

import static org.easymock.EasyMock.*;

import org.easymock.Capture;
import org.easymock.IMocksControl;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import ru.prolib.caelum.aggregator.AggregatorType;
import ru.prolib.caelum.aggregator.IAggregator;
import ru.prolib.caelum.aggregator.kafka.KafkaAggregatorBuilder.Objects;
import ru.prolib.caelum.aggregator.kafka.utils.IRecoverableStreamsService;
import ru.prolib.caelum.aggregator.kafka.utils.RecoverableStreamsService;
import ru.prolib.caelum.aggregator.kafka.utils.RecoverableStreamsServiceStarter;
import ru.prolib.caelum.core.CompositeService;
import ru.prolib.caelum.core.IService;
import ru.prolib.caelum.core.Period;
import ru.prolib.caelum.core.Periods;
import ru.prolib.caelum.itemdb.kafka.utils.KafkaUtils;

public class KafkaAggregatorBuilderTest {
	@Rule public ExpectedException eex = ExpectedException.none();
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
		config = new KafkaAggregatorConfig(new Periods());
		registryMock = control.createMock(KafkaStreamsRegistry.class);
		servicesMock = control.createMock(CompositeService.class);
		streamsServiceMock = control.createMock(RecoverableStreamsService.class);
		aggregatorMock = control.createMock(IAggregator.class);
		mutexMock = control.createMock(Lock.class);
		objects = new Objects();
		service = new KafkaAggregatorBuilder(objects);
		descr = new KafkaAggregatorDescr(AggregatorType.ITEM, Period.M6, "source", "taerget", "store");
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
		eex.expect(IllegalStateException.class);
		eex.expectMessage("Kafka utils was not defined");
		
		objects.getUtils();
	}
	
	@Test
	public void testObjects_GetUtils() {
		assertSame(objects, objects.setUtils(utils));
		
		assertSame(utils, objects.getUtils());
	}
	
	@Test
	public void testObjects_GetTopologyBuilder_ShouldThrowsIfNotDefined() {
		eex.expect(IllegalStateException.class);
		eex.expectMessage("Topology builder was not defined");
		
		objects.getTopologyBuilder();
	}
	
	@Test
	public void testObjects_GetTopologyBuilder() {
		assertSame(objects, objects.setTopologyBuilder(topologyBuilderMock));
		
		assertSame(topologyBuilderMock, objects.getTopologyBuilder());
	}
	
	@Test
	public void testObjects_GetConfig_ShouldThrowsIfNotDefined() {
		eex.expect(IllegalStateException.class);
		eex.expectMessage("Configuration was not defined");
		
		objects.getConfig();
	}
	
	@Test
	public void testObjects_GetConfig() {
		assertSame(objects, objects.setConfig(config));
		
		assertSame(config, objects.getConfig());
	}
	
	@Test
	public void testObjects_GetStreamsRegistry_ShouldThrowsIfNotDefined() {
		eex.expect(IllegalStateException.class);
		eex.expectMessage("Streams registry was not defined");
		
		objects.getStreamsRegistry();
	}
	
	@Test
	public void testObjects_GetStreamsRegistry() {
		assertSame(objects, objects.setStreamsRegistry(registryMock));

		assertSame(registryMock, objects.getStreamsRegistry());
	}
	
	@Test
	public void testObjects_GetServices_ShouldThrowsIfNotDefined() {
		eex.expect(IllegalStateException.class);
		eex.expectMessage("Services was not defined");
		
		objects.getServices();
	}
	
	@Test
	public void testObjects_GetServices() {
		assertSame(objects, objects.setServices(servicesMock));
		
		assertSame(servicesMock, objects.getServices());
	}
	
	@Test
	public void testObjects_GetCleanUpMutex_ShouldThrowsIfNotDefined() {
		eex.expect(IllegalStateException.class);
		eex.expectMessage("CleanUp mutex was not defined");
		
		objects.getCleanUpMutex();
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
	}
	
	@Test
	public void testCreateAggregator() {
		objects.setConfig(config).setUtils(utils);
		
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
		config.getProperties().put("caelum.aggregator.aggregation.period", "M5");
		config.getProperties().put("caelum.aggregator.kafka.source.topic", "source-data");
		config.getProperties().put("caelum.aggregator.kafka.pfx.application.id", "MyApp-");
		config.getProperties().put("caelum.aggregator.kafka.pfx.target.topic", "target-");
		config.getProperties().put("caelum.aggregator.kafka.pfx.aggregation.store", "store-");
		config.getProperties().put("caelum.aggregator.kafka.default.timeout", "25000");
		objects.setConfig(config).setServices(servicesMock);
		KafkaAggregatorDescr expected_descr =
				new KafkaAggregatorDescr(AggregatorType.ITEM, Period.M5, "source-data", "target-m5", "store-m5");
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
