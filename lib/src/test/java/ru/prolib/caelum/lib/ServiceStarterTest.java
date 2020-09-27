package ru.prolib.caelum.lib;

import static org.junit.Assert.*;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.log4j.BasicConfigurator;

import static org.easymock.EasyMock.*;

import org.easymock.IMocksControl;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class ServiceStarterTest {
	
	@BeforeClass
	public static void setUpBeforeClass() {
		BasicConfigurator.resetConfiguration();
		BasicConfigurator.configure();
	}
	
	IMocksControl control;
	IService serviceMock1, serviceMock2;
	ServiceStarter service;

	@Before
	public void setUp() throws Exception {
		control = createStrictControl();
		serviceMock1 = control.createMock(IService.class);
		serviceMock2 = control.createMock(IService.class);
		service = new ServiceStarter(serviceMock1);
	}

	@Test
	public void testRun() {
		serviceMock1.start();
		control.replay();
		
		service.run();
		
		control.verify();
	}
	
	@Test
	public void testRun_ShouldCatchExceptionIfItWasThrown() {
		serviceMock1.start();
		expectLastCall().andThrow(new RuntimeException("Test error"));
		control.replay();
		
		service.run();
		
		control.verify();
	}
	
	@Test
	public void testHashCode() {
		int expected = new HashCodeBuilder(113801, 59)
				.append(serviceMock1)
				.build();
		
		assertEquals(expected, service.hashCode());
	}
	
	@SuppressWarnings("unlikely-arg-type")
	@Test
	public void testEquals() {
		assertTrue(service.equals(service));
		assertTrue(service.equals(new ServiceStarter(serviceMock1)));
		assertFalse(service.equals(null));
		assertFalse(service.equals(this));
		assertFalse(service.equals(new ServiceStarter(serviceMock2)));
	}

}
