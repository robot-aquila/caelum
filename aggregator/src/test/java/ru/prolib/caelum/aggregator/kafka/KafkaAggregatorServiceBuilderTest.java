package ru.prolib.caelum.aggregator.kafka;

import static org.junit.Assert.*;

import java.util.Properties;

import static org.easymock.EasyMock.*;
import static org.hamcrest.Matchers.*;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.kafka.streams.Topology;
import org.easymock.Capture;
import org.easymock.IMocksControl;
import org.junit.Before;
import org.junit.Test;

import ru.prolib.caelum.aggregator.IAggregatorService;
import ru.prolib.caelum.core.CompositeService;
import ru.prolib.caelum.core.IService;

public class KafkaAggregatorServiceBuilderTest {
	IMocksControl control;
	KafkaAggregatorStreamBuilder streamBuilderMock;
	CompositeService servicesMock;
	KafkaAggregatorConfig config1, config2, config3, config4;
	Topology topologyMock1, topologyMock2, topologyMock3;
	IService serviceMock1, serviceMock2, serviceMock3;
	KafkaAggregatorServiceBuilder service, mockedService;

	@Before
	public void setUp() throws Exception {
		control = createStrictControl();
		streamBuilderMock = control.createMock(KafkaAggregatorStreamBuilder.class);
		servicesMock = control.createMock(CompositeService.class);
		config1 = new KafkaAggregatorConfig();
		config2 = new KafkaAggregatorConfig();
		config3 = new KafkaAggregatorConfig();
		config4 = new KafkaAggregatorConfig();
		topologyMock1 = control.createMock(Topology.class);
		topologyMock2 = control.createMock(Topology.class);
		topologyMock3 = control.createMock(Topology.class);
		serviceMock1 = control.createMock(IService.class);
		serviceMock2 = control.createMock(IService.class);
		serviceMock3 = control.createMock(IService.class);
		service = new KafkaAggregatorServiceBuilder(streamBuilderMock);
		mockedService = partialMockBuilder(KafkaAggregatorServiceBuilder.class)
				.addMockedMethod("createConfig")
				.withConstructor(KafkaAggregatorStreamBuilder.class)
				.withArgs(streamBuilderMock)
				.createMock();
	}
	
	@Test
	public void testCreateConfig() {
		KafkaAggregatorConfig actual = service.createConfig();
		
		assertNotNull(actual);
	}
	
	@Test
	public void testBuild() throws Exception {
		Capture<String> capArg1 = newCapture(), capArg2 = newCapture();
		Capture<KafkaAggregatorRegistry> capReg1 = newCapture(), capReg2 = newCapture(), capReg3 = newCapture();
		config1 = new KafkaAggregatorConfig() {
			@Override
			public void load(String default_config_file, String config_file) {
				capArg1.setValue(default_config_file);
				capArg2.setValue(config_file);
			}
		};
		// duplicates should be ignored
		config1.getProperties().put("caelum.aggregator.aggregation.period", " M1, M5, H1, M1, H1, M5, M5");
		config1.getProperties().put("caelum.aggregator.list.tuples.limit", "400");
		expect(mockedService.createConfig()).andReturn(config1);
		// create M1 aggregator
		expect(mockedService.createConfig()).andReturn(config2);
		expect(streamBuilderMock.createItemAggregatorTopology(config2)).andReturn(topologyMock1);
		expect(streamBuilderMock.buildItemAggregatorStreamsService(capture(capReg1), same(topologyMock1), same(config2)))
			.andReturn(serviceMock1);
		expect(servicesMock.register(serviceMock1)).andReturn(servicesMock);
		// create M5 aggregator
		expect(mockedService.createConfig()).andReturn(config3);
		expect(streamBuilderMock.createItemAggregatorTopology(config3)).andReturn(topologyMock2);
		expect(streamBuilderMock.buildItemAggregatorStreamsService(capture(capReg2), same(topologyMock2), same(config3)))
			.andReturn(serviceMock2);
		expect(servicesMock.register(serviceMock2)).andReturn(servicesMock);
		// create H1 aggregator
		expect(mockedService.createConfig()).andReturn(config4);
		expect(streamBuilderMock.createItemAggregatorTopology(config4)).andReturn(topologyMock3);
		expect(streamBuilderMock.buildItemAggregatorStreamsService(capture(capReg3), same(topologyMock3), same(config4)))
			.andReturn(serviceMock3);
		expect(servicesMock.register(serviceMock3)).andReturn(servicesMock);
		control.replay();
		replay(mockedService);
		
		IAggregatorService actual = mockedService.build("kappa.props", "beta.props", servicesMock);
		
		verify(mockedService);
		control.verify();
		assertThat(actual, is(instanceOf(KafkaAggregatorService.class)));
		KafkaAggregatorService x = (KafkaAggregatorService) actual;
		assertEquals(400, x.getMaxLimit());
		assertEquals("kappa.props", capArg1.getValue());
		assertEquals("beta.props", capArg2.getValue());
		assertSame(x.getRegistry(), capReg1.getValue());
		assertSame(x.getRegistry(), capReg2.getValue());
		assertSame(x.getRegistry(), capReg3.getValue());
		Properties expected_props = new Properties();
		expected_props.putAll(config1.getProperties());
		expected_props.put("caelum.aggregator.aggregation.period", "M1");
		assertEquals(expected_props, config2.getProperties());
		expected_props.put("caelum.aggregator.aggregation.period", "M5");
		assertEquals(expected_props, config3.getProperties());
		expected_props.put("caelum.aggregator.aggregation.period", "H1");
		assertEquals(expected_props, config4.getProperties());
	}
	
	@Test
	public void testHashCode() {
		int expected = new HashCodeBuilder(59710737, 15)
				.append(streamBuilderMock)
				.build();
		
		assertEquals(expected, service.hashCode());
	}
	
	@Test
	public void testEquals() {
		assertTrue(service.equals(service));
		assertTrue(service.equals(new KafkaAggregatorServiceBuilder(streamBuilderMock)));
		assertFalse(service.equals(null));
		assertFalse(service.equals(this));
		assertFalse(service.equals(new KafkaAggregatorServiceBuilder(new KafkaAggregatorStreamBuilder())));
	}

}
