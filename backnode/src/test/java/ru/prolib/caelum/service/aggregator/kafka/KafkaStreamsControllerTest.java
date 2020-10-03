package ru.prolib.caelum.service.aggregator.kafka;

import static org.junit.Assert.*;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.Properties;
import java.util.concurrent.locks.Lock;

import static org.hamcrest.Matchers.*;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.easymock.IMocksControl;

import static org.easymock.EasyMock.*;

import org.junit.Before;
import org.junit.Test;

import ru.prolib.caelum.lib.HostInfo;
import ru.prolib.caelum.lib.Interval;
import ru.prolib.caelum.lib.Intervals;
import ru.prolib.caelum.lib.kafka.KafkaItemSerdes;
import ru.prolib.caelum.service.AggregatorType;
import ru.prolib.caelum.service.GeneralConfig;
import ru.prolib.caelum.service.aggregator.kafka.utils.IRecoverableStreamsHandler;
import ru.prolib.caelum.service.aggregator.kafka.utils.IRecoverableStreamsHandlerListener;
import ru.prolib.caelum.service.aggregator.kafka.utils.RecoverableStreamsHandler;
import ru.prolib.caelum.service.itemdb.kafka.utils.KafkaUtils;

public class KafkaStreamsControllerTest {
	IMocksControl control;
	KafkaAggregatorTopologyBuilder builderMock;
	Topology topologyMock;
	KafkaStreamsRegistry registryMock;
	KafkaUtils utilsMock;
	IRecoverableStreamsHandlerListener listenerMock;
	RecoverableStreamsHandler handlerMock;
	KafkaStreams streamsMock;
	Lock mutexMock;
	GeneralConfig gconfMock;
	KafkaAggregatorConfig config;
	KafkaAggregatorDescr descr;
	Intervals intervals;
	KafkaStreamsController service;

	@Before
	public void setUp() throws Exception {
		control = createStrictControl();
		builderMock = control.createMock(KafkaAggregatorTopologyBuilder.class);
		topologyMock = control.createMock(Topology.class);
		registryMock = control.createMock(KafkaStreamsRegistry.class);
		utilsMock = control.createMock(KafkaUtils.class);
		listenerMock = control.createMock(IRecoverableStreamsHandlerListener.class);
		handlerMock = control.createMock(RecoverableStreamsHandler.class);
		streamsMock = control.createMock(KafkaStreams.class);
		mutexMock = control.createMock(Lock.class);
		gconfMock = control.createMock(GeneralConfig.class);
		config = new KafkaAggregatorConfig(Interval.M15, gconfMock);
		descr = new KafkaAggregatorDescr(AggregatorType.ITEM, Interval.M15, "foo", "bar", "foo-store");
		service = new KafkaStreamsController(descr, builderMock, config, registryMock, mutexMock, utilsMock);
	}
	
	@Test
	public void testGetters() {
		assertEquals(descr, service.getDescriptor());
		assertSame(builderMock, service.getTopologyBuilder());
		assertSame(config, service.getConfig());
		assertSame(registryMock, service.getStreamsRegistry());
		assertSame(mutexMock, service.getCleanUpMutex());
		assertSame(utilsMock, service.getUtils());
	}
	
	@Test
	public void testBuild() {
		expect(gconfMock.getAggregatorKafkaApplicationIdPrefix()).andStubReturn("aggregator-");
		expect(gconfMock.getKafkaBootstrapServers()).andStubReturn("localhost:4238");
		expect(gconfMock.getKafkaStateDir()).andStubReturn("/foo/bar");
		expect(builderMock.buildTopology(config)).andStubReturn(topologyMock);
		expect(gconfMock.getAggregatorKafkaNumStreamThreads()).andStubReturn(3);
		expect(gconfMock.getHttpInfo()).andStubReturn(new HostInfo("127.15.19.8", 4529));
		expect(gconfMock.getAggregatorKafkaLingerMs()).andStubReturn(40L);
		expect(gconfMock.getDefaultTimeout()).andStubReturn(30000L);
		Properties props = new Properties();
		props.put("application.id", "aggregator-m15");
		props.put("bootstrap.servers", "localhost:4238");
		props.put("default.key.serde", KafkaItemSerdes.keySerde().getClass());
		props.put("default.value.serde", KafkaItemSerdes.itemSerde().getClass());
		props.put("state.dir", "/foo/bar");
		props.put("num.stream.threads", "3");
		props.put("application.server", "127.15.19.8:4529");
		props.put("linger.ms", "40");
		props.put("processing.guarantee", "exactly_once");
		expect(utilsMock.createStreams(topologyMock, props)).andReturn(streamsMock);
		control.replay();
		
		IRecoverableStreamsHandler actual = service.build(listenerMock);
		
		control.verify();
		assertNotNull(actual);
		assertThat(actual, is(instanceOf(RecoverableStreamsHandler.class)));
		RecoverableStreamsHandler x = (RecoverableStreamsHandler) actual;
		assertSame(streamsMock, x.getStreams());
		assertEquals("aggregator-m15", x.getServiceName());
		assertSame(listenerMock, x.getStateListener());
		assertEquals(30000L, x.getShutdownTimeout());
		assertSame(mutexMock, x.getCleanUpMutex());
	}
	
	@Test
	public void testOnRunning() {
		expect(handlerMock.getStreams()).andReturn(streamsMock);
		registryMock.register(descr, streamsMock);
		control.replay();
		
		service.onRunning(handlerMock);
		
		control.verify();
	}

	@Test
	public void testOnClose() {
		registryMock.deregister(descr);
		control.replay();
		
		service.onClose(handlerMock);
		
		control.verify();
	}
	
	@Test
	public void testOnAvailable() {
		expect(handlerMock.available()).andReturn(true);
		registryMock.setAvailability(descr, true);
		control.replay();
		
		service.onAvailable(handlerMock);
		
		control.verify();
	}
	
	@Test
	public void testOnUnavailable() {
		expect(handlerMock.available()).andReturn(false);
		registryMock.setAvailability(descr, false);
		control.replay();
		
		service.onUnavailable(handlerMock);

		control.verify();
	}

}
