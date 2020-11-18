package ru.prolib.caelum.service.itemdb.kafka;

import static org.junit.Assert.*;
import static org.hamcrest.MatcherAssert.assertThat;

import java.time.Clock;
import java.util.Properties;

import static org.easymock.EasyMock.*;
import static org.hamcrest.Matchers.*;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.easymock.IMocksControl;
import org.junit.Before;
import org.junit.Test;

import ru.prolib.caelum.lib.kafka.KafkaItem;
import ru.prolib.caelum.service.GeneralConfig;
import ru.prolib.caelum.service.IBuildingContext;
import ru.prolib.caelum.service.itemdb.IItemDatabaseService;
import ru.prolib.caelum.service.itemdb.kafka.utils.KafkaProducerService;
import ru.prolib.caelum.service.itemdb.kafka.utils.KafkaUtils;

public class KafkaItemDatabaseServiceBuilderTest {
	IMocksControl control;
	GeneralConfig configMock;
	KafkaItemDatabaseService serviceMock;
	KafkaUtils utilsMock;
	KafkaProducer<String, KafkaItem> producerMock;
	IBuildingContext contextMock;
	Clock clockMock;
	KafkaItemDatabaseServiceBuilder service, mockedService;

	@Before
	public void setUp() throws Exception {
		control = createStrictControl();
		configMock = control.createMock(GeneralConfig.class);
		serviceMock = control.createMock(KafkaItemDatabaseService.class);
		utilsMock = control.createMock(KafkaUtils.class);
		producerMock = control.createMock(KafkaProducer.class);
		contextMock = control.createMock(IBuildingContext.class);
		clockMock = control.createMock(Clock.class);
		mockedService = partialMockBuilder(KafkaItemDatabaseServiceBuilder.class)
				.withConstructor(KafkaUtils.class, Clock.class)
				.withArgs(utilsMock, clockMock)
				.addMockedMethod("createProducer", GeneralConfig.class)
				.addMockedMethod("createService", GeneralConfig.class, KafkaProducer.class)
				.createMock(control);
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
	public void testCreateProducer() {
		expect(configMock.getKafkaBootstrapServers()).andStubReturn("kukumbara:9051");
		expect(configMock.getItemServiceKafkaTransactionalId()).andStubReturn("xxx-yyy");
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kukumbara:9051");
		props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "xxx-yyy");
		expect(utilsMock.createProducer(props)).andReturn(producerMock);
		control.replay();

		KafkaProducer<String, KafkaItem> actual = service.createProducer(configMock);
		
		control.verify();
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
		expect(contextMock.getConfig()).andStubReturn(configMock);
		expect(mockedService.createProducer(configMock)).andReturn(producerMock);
		expect(contextMock.registerService(new KafkaProducerService(producerMock))).andReturn(contextMock);
		expect(mockedService.createService(configMock, producerMock)).andReturn(serviceMock);
		control.replay();
		
		IItemDatabaseService actual = mockedService.build(contextMock);
		
		control.verify();
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
