package ru.prolib.caelum.core;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;

import static org.easymock.EasyMock.*;

import org.easymock.IMocksControl;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class CompositeServiceTest {
	@Rule
	public ExpectedException eex = ExpectedException.none();
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
		eex.expect(ServiceException.class);
		eex.expectMessage("Cannot register when started");
		
		service.register(serviceMock2);
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
		eex.expect(ServiceException.class);
		eex.expectMessage("Service already started");
		
		service.start();
	}
	
	@Test
	public void testStart_RollbackOnException() {
		Exception e = null;
		service.register(serviceMock1);
		service.register(serviceMock2);
		service.register(serviceMock3);
		serviceMock1.start();
		serviceMock2.start();
		expectLastCall().andThrow(e = new RuntimeException("Test error"));
		serviceMock1.stop();
		control.replay();
		eex.expect(ServiceException.class);
		eex.expectCause(org.hamcrest.Matchers.<Exception>equalTo(e));
		eex.expectMessage("Error starting service");
		
		service.start();
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
		Exception e = null;
		service.register(serviceMock1);
		service.register(serviceMock2);
		service.register(serviceMock3);
		service.start();
		control.resetToStrict();
		serviceMock3.stop();
		serviceMock2.stop();
		expectLastCall().andThrow(new RuntimeException("Test error 1"));
		serviceMock1.stop();
		expectLastCall().andThrow(e = new RuntimeException("Test error 2"));
		control.replay();
		eex.expect(ServiceException.class);
		eex.expectCause(org.hamcrest.Matchers.<Exception>equalTo(e));
		eex.expectMessage("At least one error while stopping service");
		
		service.stop();
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
