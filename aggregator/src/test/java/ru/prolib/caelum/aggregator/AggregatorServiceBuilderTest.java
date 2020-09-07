package ru.prolib.caelum.aggregator;

import static org.junit.Assert.*;
import static org.easymock.EasyMock.*;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.List;
import java.util.Properties;

import org.easymock.Capture;
import org.easymock.IMocksControl;
import org.junit.Before;
import org.junit.Test;

import ru.prolib.caelum.core.CompositeService;
import ru.prolib.caelum.core.Interval;

public class AggregatorServiceBuilderTest {
	
	static class TestService implements IAggregatorService {
		private final String default_config_file, config_file;
		private final CompositeService services;
		
		public TestService(String default_config_file, String config_file, CompositeService services) {
			this.default_config_file = default_config_file;
			this.config_file = config_file;
			this.services = services;
		}

		@Override
		public AggregatedDataResponse fetch(AggregatedDataRequest request) {
			throw new UnsupportedOperationException();
		}

		@Override
		public void clear(boolean global) {
			throw new UnsupportedOperationException();
		}

		@Override
		public List<Interval> getAggregationIntervals() {
			throw new UnsupportedOperationException();
		}
		
		@Test
		public List<AggregatorStatus> getAggregatorStatus() {
			throw new UnsupportedOperationException();
		}
		
	}
	
	static class TestBuilder implements IAggregatorServiceBuilder {

		@Override
		public IAggregatorService build(String default_config_file, String config_file, CompositeService services) {
			return new TestService(default_config_file, config_file, services);
		}
		
	}
	
	IMocksControl control;
	AggregatorConfig configStub;
	CompositeService servicesMock;
	AggregatorServiceBuilder service, mockedService;

	@Before
	public void setUp() throws Exception {
		control = createStrictControl();
		configStub = new AggregatorConfig();
		servicesMock = control.createMock(CompositeService.class);
		service = new AggregatorServiceBuilder();
		mockedService = partialMockBuilder(AggregatorServiceBuilder.class)
				.addMockedMethod("createConfig")
				.createMock();
	}
	
	@Test
	public void testCreateConfig() {
		AggregatorConfig actual = service.createConfig();
		
		assertNotNull(actual);
	}
	
	@Test
	public void testCreateBuilder() throws Exception {
		IAggregatorServiceBuilder actual = service.createBuilder(TestBuilder.class.getName());
		
		assertNotNull(actual);
		assertThat(actual, is(instanceOf(TestBuilder.class)));
	}

	@Test
	public void testBuild() throws Exception {
		final Capture<String> cap1 = newCapture(), cap2 = newCapture();
		configStub = new AggregatorConfig() {
			@Override
			public void load(String default_config_file, String config_file) {
				cap1.setValue(default_config_file);
				cap2.setValue(config_file);
			}
		};
		Properties props = configStub.getProperties();
		props.put("caelum.aggregator.builder", TestBuilder.class.getName());
		expect(mockedService.createConfig()).andReturn(configStub);
		control.replay();
		replay(mockedService);
		
		IAggregatorService actual = mockedService.build("bumba.props", "balboa.props", servicesMock);
		
		verify(mockedService);
		control.verify();
		assertNotNull(actual);
		assertThat(actual, is(instanceOf(TestService.class)));
		TestService x = (TestService) actual;
		assertEquals("bumba.props", x.default_config_file);
		assertEquals("balboa.props", x.config_file);
		assertSame(servicesMock, x.services);
	}
	
	@Test
	public void testHashCode() {
		int expected = 20881263;
		
		assertEquals(expected, service.hashCode());
	}
	
	@SuppressWarnings("unlikely-arg-type")
	@Test
	public void testEquals() {
		assertTrue(service.equals(service));
		assertTrue(service.equals(new AggregatorServiceBuilder()));
		assertFalse(service.equals(null));
		assertFalse(service.equals(this));
	}

}
