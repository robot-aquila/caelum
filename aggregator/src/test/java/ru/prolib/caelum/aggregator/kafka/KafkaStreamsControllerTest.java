package ru.prolib.caelum.aggregator.kafka;

import static org.junit.Assert.*;
import static org.hamcrest.Matchers.*;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.easymock.IMocksControl;

import static org.easymock.EasyMock.*;

import org.junit.Before;
import org.junit.Test;

import ru.prolib.caelum.aggregator.AggregatorType;
import ru.prolib.caelum.aggregator.kafka.utils.IRecoverableStreamsHandler;
import ru.prolib.caelum.aggregator.kafka.utils.IRecoverableStreamsHandlerListener;
import ru.prolib.caelum.aggregator.kafka.utils.RecoverableStreamsHandler;
import ru.prolib.caelum.core.Period;
import ru.prolib.caelum.core.Periods;
import ru.prolib.caelum.itemdb.kafka.utils.KafkaUtils;

public class KafkaStreamsControllerTest {
	IMocksControl control;
	KafkaAggregatorTopologyBuilder builderMock;
	Topology topologyMock;
	KafkaStreamsRegistry registryMock;
	KafkaUtils utilsMock;
	IRecoverableStreamsHandlerListener listenerMock;
	RecoverableStreamsHandler handlerMock;
	KafkaStreams streamsMock;
	KafkaAggregatorConfig config;
	KafkaAggregatorDescr descr;
	Periods periods;
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
		config = new KafkaAggregatorConfig(periods = new Periods());
		config.getProperties().put("caelum.aggregator.aggregation.period", "M15");
		config.getProperties().put("caelum.aggregator.kafka.default.timeout", "30000");
		descr = new KafkaAggregatorDescr(AggregatorType.ITEM, Period.M15, "foo", "bar", "foo-store");
		service = new KafkaStreamsController(descr, builderMock, config, registryMock, utilsMock);
	}
	
	@Test
	public void testGetters() {
		assertEquals(descr, service.getDescriptor());
		assertSame(builderMock, service.getTopologyBuilder());
		assertSame(config, service.getConfig());
		assertSame(registryMock, service.getStreamsRegistry());
		assertSame(utilsMock, service.getUtils());
	}
	
	@Test
	public void testBuild() {
		expect(builderMock.buildTopology(config)).andReturn(topologyMock);
		expect(utilsMock.createStreams(topologyMock, config.getKafkaProperties())).andReturn(streamsMock);
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

}