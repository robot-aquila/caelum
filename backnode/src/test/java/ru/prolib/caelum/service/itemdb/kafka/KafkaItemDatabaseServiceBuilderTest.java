package ru.prolib.caelum.service.itemdb.kafka;

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

import ru.prolib.caelum.lib.kafka.KafkaItem;
import ru.prolib.caelum.service.IBuildingContext;
import ru.prolib.caelum.service.itemdb.IItemDatabaseService;
import ru.prolib.caelum.service.itemdb.kafka.utils.KafkaProducerService;
import ru.prolib.caelum.service.itemdb.kafka.utils.KafkaUtils;

@SuppressWarnings("unchecked")
public class KafkaItemDatabaseServiceBuilderTest {
	IMocksControl control;
	KafkaItemDatabaseConfig configMock;
	KafkaItemDatabaseService serviceMock;
	KafkaUtils utilsMock;
	KafkaProducer<String, KafkaItem> producerMock;
	IBuildingContext contextMock;
	Clock clockMock;
	KafkaItemDatabaseServiceBuilder service, mockedService;

	@Before
	public void setUp() throws Exception {
		control = createStrictControl();
		configMock = control.createMock(KafkaItemDatabaseConfig.class);
		serviceMock = control.createMock(KafkaItemDatabaseService.class);
		utilsMock = control.createMock(KafkaUtils.class);
		producerMock = control.createMock(KafkaProducer.class);
		contextMock = control.createMock(IBuildingContext.class);
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
		expect(contextMock.getDefaultConfigFileName()).andStubReturn("bururum.props");
		expect(contextMock.getConfigFileName()).andStubReturn("tutumbr.props");
		configMock.load("bururum.props", "tutumbr.props");
		expect(configMock.getProducerKafkaProperties()).andReturn(propsMock);
		expect(utilsMock.createProducer(propsMock)).andReturn(producerMock);
		expect(contextMock.registerService(new KafkaProducerService(producerMock))).andReturn(contextMock);
		expect(mockedService.createService(configMock, producerMock)).andReturn(serviceMock);
		replay(mockedService);
		control.replay();
		
		IItemDatabaseService actual = mockedService.build(contextMock);
		
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
