package ru.prolib.caelum.service.aggregator;

import static org.junit.Assert.*;
import static org.easymock.EasyMock.*;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.List;
import org.easymock.IMocksControl;
import org.junit.Before;
import org.junit.Test;

import ru.prolib.caelum.lib.Interval;
import ru.prolib.caelum.service.AggregatedDataRequest;
import ru.prolib.caelum.service.AggregatedDataResponse;
import ru.prolib.caelum.service.AggregatorStatus;
import ru.prolib.caelum.service.GeneralConfig;
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
	GeneralConfig configMock;
	IBuildingContext contextMock;
	AggregatorServiceBuilder service;

	@Before
	public void setUp() throws Exception {
		control = createStrictControl();
		configMock = control.createMock(GeneralConfig.class);
		contextMock = control.createMock(IBuildingContext.class);
		service = new AggregatorServiceBuilder();
	}
	
	@Test
	public void testCreateBuilder() throws Exception {
		IAggregatorServiceBuilder actual = service.createBuilder(TestBuilder.class.getName());
		
		assertNotNull(actual);
		assertThat(actual, is(instanceOf(TestBuilder.class)));
	}

	@Test
	public void testBuild() throws Exception {
		expect(contextMock.getConfig()).andReturn(configMock);
		expect(configMock.getAggregatorServiceBuilder()).andReturn(TestBuilder.class.getName());
		control.replay();
		
		IAggregatorService actual = service.build(contextMock);
		
		control.verify();
		assertNotNull(actual);
		assertThat(actual, is(instanceOf(TestService.class)));
		TestService x = (TestService) actual;
		assertSame(contextMock, x.context);
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
