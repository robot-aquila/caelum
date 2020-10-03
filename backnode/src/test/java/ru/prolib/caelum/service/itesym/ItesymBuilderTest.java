package ru.prolib.caelum.service.itesym;

import static org.junit.Assert.*;
import static org.easymock.EasyMock.*;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.Properties;

import org.apache.kafka.clients.consumer.KafkaConsumer;


import org.easymock.Capture;
import org.easymock.IMocksControl;
import org.junit.Before;
import org.junit.Test;

import ru.prolib.caelum.lib.IService;
import ru.prolib.caelum.service.GeneralConfigImpl;
import ru.prolib.caelum.service.IBuildingContext;
import ru.prolib.caelum.service.ICaelum;
import ru.prolib.caelum.service.IExtension;

public class ItesymBuilderTest {
	IMocksControl control;
	KafkaConsumer<String, byte[]> consumerMock;
	GeneralConfigImpl config;
	ItesymBuilder mockedService, service;
	IBuildingContext contextMock;
	ICaelum caelumMock;
	Thread threadMock;

	@SuppressWarnings("unchecked")
	@Before
	public void setUp() throws Exception {
		config = new GeneralConfigImpl();
		control = createStrictControl();
		consumerMock = control.createMock(KafkaConsumer.class);
		service = new ItesymBuilder();
		mockedService = partialMockBuilder(ItesymBuilder.class)
				.withConstructor()
				.addMockedMethod("createConsumer", Properties.class)
				.addMockedMethod("createThread", String.class, Runnable.class)
				.createMock(control);
		contextMock = control.createMock(IBuildingContext.class);
		caelumMock = control.createMock(ICaelum.class);
		threadMock = control.createMock(Thread.class);
	}
	
	@Test
	public void testCreateConsumer() {
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:8082");
		props.put("group.id", "test-app");

		KafkaConsumer<String, byte[]> actual = service.createConsumer(props);
		
		assertNotNull(actual);
	}
	
	@Test
	public void testCreateThread() throws Exception {
		Runnable runnableMock = control.createMock(Runnable.class);
		runnableMock.run();
		control.replay();
		
		Thread actual = service.createThread("foobar", runnableMock);
		
		assertNotNull(actual);
		assertEquals("foobar", actual.getName());
		actual.start();
		actual.join(1000L);
		assertFalse(actual.isAlive());
		control.verify();
	}
	
	@Test
	public void testBuild() throws Exception {
		expect(contextMock.getConfig()).andStubReturn(config);
		expect(contextMock.getCaelum()).andStubReturn(caelumMock);
		config.setKafkaBootstrapServers("172.94.13.37:8082")
			.setItesymKafkaGroupId("tutumbr")
			.setKafkaPollTimeout(5000L)
			.setShutdownTimeout(10000L)
			.setItemsTopicName("bambr");
		Properties props = new Properties();
		props.put("bootstrap.servers", "172.94.13.37:8082");
		props.put("group.id", "tutumbr");
		props.put("enable.auto.commit", "false");
		props.put("isolation.level", "read_committed");
		expect(mockedService.createConsumer(eq(props))).andReturn(consumerMock);
		Capture<Runnable> cap1 = newCapture();
		expect(mockedService.createThread(eq("tutumbr"), capture(cap1))).andReturn(threadMock);
		Capture<IService> cap2 = newCapture();
		expect(contextMock.registerService(capture(cap2))).andReturn(contextMock);
		control.replay();
		
		IExtension actual = mockedService.build(contextMock);
		
		control.verify();
		assertNotNull(actual);
		assertThat(actual, is(instanceOf(Itesym.class)));
		assertSame(cap1.getValue(), actual);
		assertSame(cap2.getValue(), actual);
		Itesym o = (Itesym) actual;
		assertEquals("tutumbr", o.getGroupId());
		assertSame(consumerMock, o.getConsumer());
		assertEquals("bambr", o.getSourceTopic());
		assertSame(caelumMock, o.getCaelum());
		assertEquals(5000L, o.getPollTimeout());
		assertEquals(10000L, o.getShutdownTimeout());
		assertSame(threadMock, o.getThread());
	}

}
