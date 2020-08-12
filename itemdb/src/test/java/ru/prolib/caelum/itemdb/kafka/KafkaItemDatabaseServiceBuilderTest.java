package ru.prolib.caelum.itemdb.kafka;

import static org.junit.Assert.*;
import static org.hamcrest.MatcherAssert.assertThat;

import java.time.Clock;
import java.util.Properties;

import static org.easymock.EasyMock.*;
import static org.hamcrest.Matchers.*;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.easymock.IMocksControl;
import org.junit.Before;
import org.junit.Test;

import ru.prolib.caelum.core.CompositeService;
import ru.prolib.caelum.itemdb.IItemDatabaseService;
import ru.prolib.caelum.itemdb.kafka.utils.KafkaProducerService;
import ru.prolib.caelum.itemdb.kafka.utils.KafkaUtils;

@SuppressWarnings("unchecked")
public class KafkaItemDatabaseServiceBuilderTest {
	IMocksControl control;
	KafkaItemDatabaseConfig configMock;
	KafkaItemDatabaseService serviceMock;
	KafkaUtils utilsMock;
	KafkaProducer<String, KafkaItem> producerMock;
	CompositeService servicesMock;
	Clock clockMock;
	KafkaItemDatabaseServiceBuilder service, mockedService;

	@Before
	public void setUp() throws Exception {
		control = createStrictControl();
		configMock = control.createMock(KafkaItemDatabaseConfig.class);
		serviceMock = control.createMock(KafkaItemDatabaseService.class);
		utilsMock = control.createMock(KafkaUtils.class);
		producerMock = control.createMock(KafkaProducer.class);
		servicesMock = control.createMock(CompositeService.class);
		clockMock = control.createMock(Clock.class);
		mockedService = partialMockBuilder(KafkaItemDatabaseServiceBuilder.class)
				.withConstructor(KafkaUtils.class, Clock.class)
				.withArgs(utilsMock, clockMock)
				.addMockedMethod("createConfig")
				.addMockedMethod("createService")
				.createMock();
		service = new KafkaItemDatabaseServiceBuilder(utilsMock, clockMock);
	}
	
	@Test
	public void testGetters() {
		assertSame(utilsMock, service.getUtils());
		assertSame(clockMock, service.getClock());
	}
	
	@Test
	public void testGetters_Ctor1() {
		service = new KafkaItemDatabaseServiceBuilder();
		
		assertSame(KafkaUtils.getInstance(), service.getUtils());
		assertEquals(Clock.systemUTC(), service.getClock());
	}
	
	@Test
	public void testCreateConfig() {
		KafkaItemDatabaseConfig actual = service.createConfig();
		
		assertNotNull(actual);
	}
	
	@Test
	public void testCreateService() {
		control.replay();
		
		KafkaItemDatabaseService actual = service.createService(configMock, producerMock);
		
		control.verify();
		assertNotNull(actual);
		assertSame(configMock, actual.getConfig());
		assertSame(producerMock, actual.getProducer());
		assertSame(utilsMock, actual.getUtils());
		assertSame(clockMock, actual.getClock());
	}
	
	@Test
	public void testBuild() throws Exception {
		Properties propsMock = control.createMock(Properties.class);
		expect(mockedService.createConfig()).andReturn(configMock);
		configMock.load("bururum.props", "tutumbr.props");
		expect(configMock.getProducerKafkaProperties()).andReturn(propsMock);
		expect(utilsMock.createProducer(propsMock)).andReturn(producerMock);
		expect(servicesMock.register(new KafkaProducerService(producerMock))).andReturn(servicesMock);
		expect(mockedService.createService(configMock, producerMock)).andReturn(serviceMock);
		replay(mockedService);
		control.replay();
		
		IItemDatabaseService actual = mockedService.build("bururum.props", "tutumbr.props", servicesMock);
		
		control.verify();
		verify(mockedService);
		assertNotNull(actual);
		assertThat(actual, is(instanceOf(KafkaItemDatabaseService.class)));
	}
	
	@Test
	public void testHashCode() {
		int expected = 50098172;
		
		assertEquals(expected, service.hashCode());
	}

	@SuppressWarnings("unlikely-arg-type")
	@Test
	public void testEquals() {
		assertTrue(service.equals(service));
		assertTrue(service.equals(new KafkaItemDatabaseServiceBuilder()));
		assertFalse(service.equals(null));
		assertFalse(service.equals(this));
	}

}
