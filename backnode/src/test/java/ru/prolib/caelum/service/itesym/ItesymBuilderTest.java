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

import ru.prolib.caelum.lib.CompositeService;
import ru.prolib.caelum.lib.IService;
import ru.prolib.caelum.service.ICaelum;
import ru.prolib.caelum.service.IExtension;

public class ItesymBuilderTest {
	IMocksControl control;
	KafkaConsumer<String, byte[]> consumerMock;
	ItesymConfig mockedConfig;
	ItesymBuilder mockedService, service;
	CompositeService servicesMock;
	ICaelum caelumMock;
	Thread threadMock;

	@SuppressWarnings("unchecked")
	@Before
	public void setUp() throws Exception {
		control = createStrictControl();
		consumerMock = control.createMock(KafkaConsumer.class);
		mockedConfig = partialMockBuilder(ItesymConfig.class)
				.withConstructor()
				.addMockedMethod("load", String.class, String.class)
				.createMock();
		service = new ItesymBuilder();
		mockedService = partialMockBuilder(ItesymBuilder.class)
				.withConstructor()
				.addMockedMethod("createConfig")
				.addMockedMethod("createConsumer", Properties.class)
				.addMockedMethod("createThread", String.class, Runnable.class)
				.createMock();
		servicesMock = control.createMock(CompositeService.class);
		caelumMock = control.createMock(ICaelum.class);
		threadMock = control.createMock(Thread.class);
	}
	
	@Test
	public void testCreateConfig() {
		ItesymConfig actual = service.createConfig();
		
		assertNotNull(actual);
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
		mockedConfig.getProperties().put("caelum.itesym.bootstrap.servers", "172.94.13.37:8082");
		mockedConfig.getProperties().put("caelum.itesym.group.id", "tutumbr");
		mockedConfig.getProperties().put("caelum.itesym.source.topic", "bambr");
		mockedConfig.getProperties().put("caelum.itesym.poll.timeout", "5000");
		mockedConfig.getProperties().put("caelum.itesym.shutdown.timeout", "10000");
		expect(mockedService.createConfig()).andReturn(mockedConfig);
		mockedConfig.load("foo.props", "bar.props");
		Properties expected_ak_props = new Properties();
		expected_ak_props.put("bootstrap.servers", "172.94.13.37:8082");
		expected_ak_props.put("group.id", "tutumbr");
		expected_ak_props.put("enable.auto.commit", "false");
		expected_ak_props.put("isolation.level", "read_committed");
		expect(mockedService.createConsumer(eq(expected_ak_props))).andReturn(consumerMock);
		Capture<Runnable> cap1 = newCapture();
		expect(mockedService.createThread(eq("tutumbr"), capture(cap1))).andReturn(threadMock);
		Capture<IService> cap2 = newCapture();
		expect(servicesMock.register(capture(cap2))).andReturn(servicesMock);
		control.replay();
		replay(mockedConfig);
		replay(mockedService);
		
		IExtension actual = mockedService.build("foo.props", "bar.props", servicesMock, caelumMock);
		
		verify(mockedService);
		verify(mockedConfig);
		control.verify();
		assertNotNull(actual);
		assertThat(actual, is(instanceOf(Itesym.class)));
		assertSame(cap1.getValue(), actual);
		assertSame(cap2.getValue(), actual);
		Itesym o = (Itesym) actual;
		assertEquals("tutumbr", o.getId());
		assertSame(consumerMock, o.getConsumer());
		assertEquals("bambr", o.getSourceTopic());
		assertSame(caelumMock, o.getCaelum());
		assertEquals(5000L, o.getPollTimeout());
		assertEquals(10000L, o.getShutdownTimeout());
		assertSame(threadMock, o.getThread());
	}

}
