package ru.prolib.caelum.itemdb.kafka;

import static org.junit.Assert.*;
import static org.easymock.EasyMock.*;
import static org.hamcrest.Matchers.*;

import org.easymock.IMocksControl;
import org.junit.Before;
import org.junit.Test;

import ru.prolib.caelum.core.CompositeService;
import ru.prolib.caelum.itemdb.IItemDatabaseService;

public class KafkaItemDatabaseServiceBuilderTest {
	IMocksControl control;
	KafkaItemDatabaseConfig configMock;
	KafkaItemDatabaseService serviceMock;
	KafkaItemDatabaseServiceBuilder service, mockedService;

	@Before
	public void setUp() throws Exception {
		control = createStrictControl();
		configMock = control.createMock(KafkaItemDatabaseConfig.class);
		serviceMock = control.createMock(KafkaItemDatabaseService.class);
		mockedService = partialMockBuilder(KafkaItemDatabaseServiceBuilder.class)
				.addMockedMethod("createConfig")
				.addMockedMethod("createService")
				.createMock();
		service = new KafkaItemDatabaseServiceBuilder();
	}
	
	@Test
	public void testCreateConfig() {
		KafkaItemDatabaseConfig actual = service.createConfig();
		
		assertNotNull(actual);
	}
	
	@Test
	public void testCreateService() {
		KafkaItemDatabaseService actual = service.createService(configMock);
		
		assertNotNull(actual);
		assertSame(configMock, actual.getConfig());
	}
	
	@Test
	public void testBuild() throws Exception {
		expect(mockedService.createConfig()).andReturn(configMock);
		configMock.load("bururum.props", "tutumbr.props");
		expect(mockedService.createService(configMock)).andReturn(serviceMock);
		replay(mockedService);
		
		IItemDatabaseService actual = mockedService.build("bururum.props", "tutumbr.props", new CompositeService());
		
		verify(mockedService);
		assertNotNull(actual);
		assertThat(actual, is(instanceOf(KafkaItemDatabaseService.class)));
	}
	
	@Test
	public void testHashCode() {
		int expected = 50098172;
		
		assertEquals(expected, service.hashCode());
	}

	@Test
	public void testEquals() {
		assertTrue(service.equals(service));
		assertTrue(service.equals(new KafkaItemDatabaseServiceBuilder()));
		assertFalse(service.equals(null));
		assertFalse(service.equals(this));
	}

}
