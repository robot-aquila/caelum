package ru.prolib.caelum.core;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.log4j.BasicConfigurator;

import static org.easymock.EasyMock.*;

import org.easymock.IMocksControl;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class ExecutorServiceTest {
	
	@BeforeClass
	public static void setUpBeforeClass() {
		BasicConfigurator.resetConfiguration();
		BasicConfigurator.configure();
	}
	
	IMocksControl control;
	java.util.concurrent.ExecutorService executorMock1, executorMock2;
	ExecutorService service;

	@Before
	public void setUp() throws Exception {
		control = createStrictControl();
		executorMock1 = control.createMock(java.util.concurrent.ExecutorService.class);
		executorMock2 = control.createMock(java.util.concurrent.ExecutorService.class);
		service = new ExecutorService(executorMock1, 5000L);
	}
	
	@Test
	public void testGetters() {
		assertSame(executorMock1, service.getExecutor());
		assertEquals(5000L, service.getShutdownTimeout());
	}
	
	@Test
	public void testStart() {
		control.replay();
		
		service.start();
		
		control.verify();
	}
	
	@Test
	public void testStop_OkAtFirstPhase() throws Exception {
		executorMock1.shutdown();
		expect(executorMock1.awaitTermination(5000L, TimeUnit.MILLISECONDS)).andReturn(true);
		control.replay();
		
		service.stop();
		
		control.verify();
	}
	
	@Test
	public void testStop_TimeoutWhileAwaitingTerminationAtFirstPhase() throws Exception {
		executorMock1.shutdown();
		expect(executorMock1.awaitTermination(5000L, TimeUnit.MILLISECONDS)).andReturn(false);
		expect(executorMock1.shutdownNow()).andReturn(new ArrayList<>());
		expect(executorMock1.awaitTermination(5000L, TimeUnit.MILLISECONDS)).andReturn(true);
		control.replay();
		
		service.stop();
		
		control.verify();
	}
	
	@Test
	public void testStop_TimeoutWhileAwaitingTerminationAtSecondPhase() throws Exception {
		executorMock1.shutdown();
		expect(executorMock1.awaitTermination(5000L, TimeUnit.MILLISECONDS)).andReturn(false);
		expect(executorMock1.shutdownNow()).andReturn(Arrays.asList());
		expect(executorMock1.awaitTermination(5000L, TimeUnit.MILLISECONDS)).andReturn(false);
		control.replay();
		
		service.stop();
		
		control.verify();
	}
	
	@Test
	public void testStop_InterruptedAtFirstPhase() throws Exception {
		executorMock1.shutdown();
		expect(executorMock1.awaitTermination(5000L, TimeUnit.MILLISECONDS))
			.andThrow(new InterruptedException("Test interruption"));
		expect(executorMock1.shutdownNow()).andReturn(Arrays.asList());
		control.replay();
		
		service.stop();
		
		control.verify();
	}
	
	@Test
	public void testStop_InterruptedAtSecondPhase() throws Exception {
		executorMock1.shutdown();
		expect(executorMock1.awaitTermination(5000L, TimeUnit.MILLISECONDS)).andReturn(false);
		expect(executorMock1.shutdownNow()).andReturn(Arrays.asList());
		expect(executorMock1.awaitTermination(5000L, TimeUnit.MILLISECONDS))
			.andThrow(new InterruptedException("Test interruption"));
		expect(executorMock1.shutdownNow()).andReturn(Arrays.asList());
		control.replay();
		
		service.stop();
		
		control.verify();
	}
	
	@Test
	public void testHashCode() {
		int expected = new HashCodeBuilder(1782191049, 95)
				.append(executorMock1)
				.append(5000L)
				.build();
		
		assertEquals(expected, service.hashCode());
	}

	@SuppressWarnings("unlikely-arg-type")
	@Test
	public void testEquals() {
		assertTrue(service.equals(service));
		assertTrue(service.equals(new ExecutorService(executorMock1, 5000L)));
		assertFalse(service.equals(null));
		assertFalse(service.equals(this));
		assertFalse(service.equals(new ExecutorService(executorMock2, 5000L)));
		assertFalse(service.equals(new ExecutorService(executorMock1, 7000L)));
		assertFalse(service.equals(new ExecutorService(executorMock2, 7000L)));
	}

}
