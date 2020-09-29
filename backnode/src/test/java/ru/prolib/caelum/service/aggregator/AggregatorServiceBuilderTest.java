package ru.prolib.caelum.service.aggregator;

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

import ru.prolib.caelum.lib.Interval;
import ru.prolib.caelum.service.AggregatedDataRequest;
import ru.prolib.caelum.service.AggregatedDataResponse;
import ru.prolib.caelum.service.AggregatorStatus;
import ru.prolib.caelum.service.IBuildingContext;

public class AggregatorServiceBuilderTest {
	
	static class TestService implements IAggregatorService {
		private final IBuildingContext context;
		
		public TestService(IBuildingContext context) {
			this.context = context;
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
		public IAggregatorService build(IBuildingContext context) {
			return new TestService(context);
		}
		
	}
	
	IMocksControl control;
	AggregatorConfig configStub;
	IBuildingContext contextMock;
	AggregatorServiceBuilder service, mockedService;

	@Before
	public void setUp() throws Exception {
		control = createStrictControl();
		configStub = new AggregatorConfig();
		contextMock = control.createMock(IBuildingContext.class);
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
		expect(contextMock.getDefaultConfigFileName()).andReturn("/foo/bar.props");
		expect(contextMock.getConfigFileName()).andReturn("/foo/gap.props");
		control.replay();
		replay(mockedService);
		
		IAggregatorService actual = mockedService.build(contextMock);
		
		verify(mockedService);
		control.verify();
		assertNotNull(actual);
		assertThat(actual, is(instanceOf(TestService.class)));
		TestService x = (TestService) actual;
		assertSame(contextMock, x.context);
		assertEquals("/foo/bar.props", cap1.getValue());
		assertEquals("/foo/gap.props", cap2.getValue());
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
