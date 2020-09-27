package ru.prolib.caelum.service.itemdb.kafka.utils;

import static org.junit.Assert.*;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static org.easymock.EasyMock.*;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.log4j.BasicConfigurator;
import org.easymock.IMocksControl;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import ru.prolib.caelum.lib.ServiceException;

public class KafkaCreateTopicServiceTest {
	
	@BeforeClass
	public static void setUpBeforeClass() {
		BasicConfigurator.resetConfiguration();
		BasicConfigurator.configure();
	}
	
	IMocksControl control;
	KafkaUtils utilsMock1, utilsMock2;
	AdminClient adminMock;
	Properties props1, props2;
	NewTopic topicDescr1, topicDescr2;
	KafkaCreateTopicService service;

	@Before
	public void setUp() throws Exception {
		control = createStrictControl();
		utilsMock1 = control.createMock(KafkaUtils.class);
		utilsMock2 = control.createMock(KafkaUtils.class);
		adminMock = control.createMock(AdminClient.class);
		props1 = new Properties(); props1.put("foo", "1");
		props2 = new Properties(); props1.put("foo", "2");
		topicDescr1 = new NewTopic("foo", 12, (short)1);
		topicDescr2 = new NewTopic("bar", 10, (short)3);
		service = new KafkaCreateTopicService(utilsMock1, props1, topicDescr1, 5000L);
	}
	
	@Test
	public void testGetters() {
		assertSame(utilsMock1, service.getUtils());
		assertSame(props1, service.getAdminClientProperties());
		assertEquals(topicDescr1, service.getTopicDescriptor());
		assertEquals(5000L, service.getTimeout());
	}
	
	@Test
	public void testStop_ShouldDoNothing() {
		control.replay();
		
		service.stop();
		
		control.verify();
	}
	
	@Test
	public void testStart_ShouldCreateTopic() throws Exception {
		expect(utilsMock1.createAdmin(props1)).andReturn(adminMock);
		utilsMock1.createTopic(adminMock, topicDescr1, 5000L);
		adminMock.close();
		control.replay();
		
		service.start();
		
		control.verify();
	}
	
	@Test
	public void testStart_ShouldLogAndPassInCaseOfExecutionException() throws Exception {
		expect(utilsMock1.createAdmin(props1)).andReturn(adminMock);
		utilsMock1.createTopic(adminMock, topicDescr1, 5000L);
		expectLastCall().andThrow(new ExecutionException("Test error", null));
		adminMock.close();
		control.replay();
		
		service.start();
		
		control.verify();
	}
	
	@Test
	public void testStart_ShouldThrowInCaseOfTimeoutException() throws Exception {
		expect(utilsMock1.createAdmin(props1)).andReturn(adminMock);
		utilsMock1.createTopic(adminMock, topicDescr1, 5000L);
		expectLastCall().andThrow(new TimeoutException("Test error"));
		adminMock.close();
		control.replay();
		
		ServiceException e = assertThrows(ServiceException.class, () -> service.start());
		
		control.verify();
		assertThat(e.getMessage(), is(equalTo("Unexpected exception: ")));
		assertThat(e.getCause(), is(instanceOf(TimeoutException.class)));
		assertThat(e.getCause().getMessage(), is(equalTo("Test error")));
	}
	
	@Test
	public void testStart_ShouldThrowInCaseOfInterruptedException() throws Exception {
		expect(utilsMock1.createAdmin(props1)).andReturn(adminMock);
		utilsMock1.createTopic(adminMock, topicDescr1, 5000L);
		expectLastCall().andThrow(new InterruptedException("Test error"));
		adminMock.close();
		control.replay();
		
		ServiceException e = assertThrows(ServiceException.class, () -> service.start());
		
		control.verify();
		assertThat(e.getMessage(), is(equalTo("Unexpected exception: ")));
		assertThat(e.getCause(), is(instanceOf(InterruptedException.class)));
		assertThat(e.getCause().getMessage(), is(equalTo("Test error")));
	}

	@SuppressWarnings("unlikely-arg-type")
	@Test
	public void testEquals() {
		assertTrue(service.equals(service));
		assertTrue(service.equals(new KafkaCreateTopicService(utilsMock1, props1, topicDescr1, 5000L)));
		assertFalse(service.equals(null));
		assertFalse(service.equals(this));
		assertFalse(service.equals(new KafkaCreateTopicService(utilsMock2, props1, topicDescr1, 5000L)));
		assertFalse(service.equals(new KafkaCreateTopicService(utilsMock1, props2, topicDescr1, 5000L)));
		assertFalse(service.equals(new KafkaCreateTopicService(utilsMock1, props1, topicDescr2, 5000L)));
		assertFalse(service.equals(new KafkaCreateTopicService(utilsMock1, props1, topicDescr1, 7000L)));
		assertFalse(service.equals(new KafkaCreateTopicService(utilsMock2, props2, topicDescr2, 7000L)));
	}
	
	@Test
	public void testHashCode() {
		int expected = new HashCodeBuilder(115237, 29)
				.append(utilsMock1)
				.append(props1)
				.append(topicDescr1)
				.append(5000L)
				.build();
		
		assertEquals(expected, service.hashCode());
	}

}
