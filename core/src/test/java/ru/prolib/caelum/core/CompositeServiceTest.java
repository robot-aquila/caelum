package ru.prolib.caelum.core;

import static org.junit.Assert.*;
import static org.easymock.EasyMock.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;

import org.apache.log4j.BasicConfigurator;
import org.easymock.IMocksControl;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class CompositeServiceTest {
	
	@BeforeClass
	public static void setUpBeforeClass() {
		BasicConfigurator.resetConfiguration();
		BasicConfigurator.configure();
	}
	
	IMocksControl control;
	IService serviceMock1, serviceMock2, serviceMock3;
	CompositeService service;
	ArrayList<IService> registered;
	LinkedList<IService> started;

	@Before
	public void setUp() throws Exception {
		control = createStrictControl();
		serviceMock1 = control.createMock(IService.class);
		serviceMock2 = control.createMock(IService.class);
		serviceMock3 = control.createMock(IService.class);
		registered = new ArrayList<>();
		started = new LinkedList<>();
		service = new CompositeService(registered, started);
	}
	
	@Test
	public void testRegister() {
		assertSame(service, service.register(serviceMock1));
		assertSame(service, service.register(serviceMock2));
		
		assertEquals(Arrays.asList(serviceMock1, serviceMock2), registered);
		assertEquals(Arrays.asList(), started);
	}
	
	@Test
	public void testRegister_ThrowsIfStarted() {
		service.register(serviceMock1);
		service.start();
		
		ServiceException e = assertThrows(ServiceException.class, () -> service.register(serviceMock2));
		assertEquals("Cannot register when started", e.getMessage());
	}
	
	@Test
	public void testStart() {
		service.register(serviceMock1);
		service.register(serviceMock2);
		service.register(serviceMock3);
		serviceMock1.start();
		serviceMock2.start();
		serviceMock3.start();
		control.replay();
		
		service.start();
		
		control.verify();
		assertEquals(Arrays.asList(serviceMock1, serviceMock2, serviceMock3), registered);
		assertEquals(Arrays.asList(serviceMock1, serviceMock2, serviceMock3), started);
	}
	
	@Test
	public void testStart_ThrowsIfStarted() {
		service.register(serviceMock1);
		service.start();
		
		ServiceException e = assertThrows(ServiceException.class, () -> service.start());
		assertEquals("Service already started", e.getMessage());
	}
	
	@Test
	public void testStart_RollbackOnException() {
		service.register(serviceMock1);
		service.register(serviceMock2);
		service.register(serviceMock3);
		serviceMock1.start();
		serviceMock2.start();
		expectLastCall().andThrow(new RuntimeException("Test error"));
		serviceMock1.stop();
		control.replay();
		
		ServiceException e = assertThrows(ServiceException.class, () -> service.start());
		assertEquals("Error starting service", e.getMessage());
		assertThat(e.getCause(), is(instanceOf(RuntimeException.class)));
		assertThat(e.getCause().getMessage(), is(equalTo("Test error")));
	}
	
	@Test
	public void testStart_RollbackOnException_CheckStatus() {
		service.register(serviceMock1);
		service.register(serviceMock2);
		service.register(serviceMock3);
		serviceMock1.start();
		serviceMock2.start();
		expectLastCall().andThrow(new RuntimeException("Test error"));
		serviceMock1.stop();
		control.replay();
		
		try {
			service.start();
			fail("Expected exception: " + ServiceException.class.getSimpleName());
		} catch ( ServiceException e ) {
			assertEquals(Arrays.asList(serviceMock1, serviceMock2, serviceMock3), registered);
			assertEquals(Arrays.asList(), started);
		}
	}
	
	@Test
	public void testStop() {
		service.register(serviceMock1);
		service.register(serviceMock2);
		service.register(serviceMock3);
		service.start();
		control.resetToStrict();
		serviceMock3.stop();
		serviceMock2.stop();
		serviceMock1.stop();
		control.replay();
		
		service.stop();
		
		control.verify();
		assertEquals(Arrays.asList(serviceMock1, serviceMock2, serviceMock3), registered);
		assertEquals(Arrays.asList(), started);
	}
	
	@Test
	public void testStop_ThrowsLastError() {
		service.register(serviceMock1);
		service.register(serviceMock2);
		service.register(serviceMock3);
		service.start();
		control.resetToStrict();
		serviceMock3.stop();
		serviceMock2.stop();
		expectLastCall().andThrow(new RuntimeException("Test error 1"));
		serviceMock1.stop();
		expectLastCall().andThrow(new RuntimeException("Test error 2"));
		control.replay();
		
		ServiceException e = assertThrows(ServiceException.class, () -> service.stop());
		assertEquals("At least one error while stopping service", e.getMessage());
		assertThat(e.getCause(), is(instanceOf(RuntimeException.class)));
		assertThat(e.getCause().getMessage(), is(equalTo("Test error 2")));
	}
	
	@Test
	public void testStop_IgnoreIfNotStarted() {
		service.register(serviceMock1);
		service.register(serviceMock2);
		service.register(serviceMock3);
		control.replay();
		
		service.stop();
		
		assertEquals(Arrays.asList(serviceMock1, serviceMock2, serviceMock3), registered);
		assertEquals(Arrays.asList(), started);
	}

}
