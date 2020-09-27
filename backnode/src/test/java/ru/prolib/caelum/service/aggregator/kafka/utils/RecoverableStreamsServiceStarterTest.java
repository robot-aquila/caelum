package ru.prolib.caelum.service.aggregator.kafka.utils;

import static org.junit.Assert.*;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.BasicConfigurator;

import static org.easymock.EasyMock.*;

import org.easymock.IMocksControl;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import ru.prolib.caelum.lib.ServiceException;
import ru.prolib.caelum.service.aggregator.AggregatorState;

public class RecoverableStreamsServiceStarterTest {
	
	@BeforeClass
	public static void setUpBeforeClass() {
		BasicConfigurator.resetConfiguration();
		BasicConfigurator.configure();
	}
	
	IMocksControl control;
	CountDownLatch threadSignal;
	Thread thread;
	IRecoverableStreamsService streamsServiceMock;
	RecoverableStreamsServiceStarter service;

	@Before
	public void setUp() throws Exception {
		control = createStrictControl();
		threadSignal = new CountDownLatch(1);
		thread = new Thread(() -> threadSignal.countDown(), "foobar");
		streamsServiceMock = control.createMock(IRecoverableStreamsService.class);
		service = new RecoverableStreamsServiceStarter(thread, streamsServiceMock, 30000L);
	}
	
	@Test
	public void testGetters() {
		assertSame(thread, service.getThread());
		assertSame(streamsServiceMock, service.getStreamsService());
		assertEquals(30000L, service.getTimeout());
	}
	
	@Test
	public void testStart_ShouldThrowIfTimeoutWhileStarting() {
		expect(streamsServiceMock.waitForStateChangeFrom(AggregatorState.CREATED, 30000L)).andReturn(false);
		control.replay();
		
		ServiceException e = assertThrows(ServiceException.class, () -> service.start());
		assertEquals("Timeout while starting service", e.getMessage());
	}
	
	@Test
	public void testStart() throws Exception {
		expect(streamsServiceMock.waitForStateChangeFrom(AggregatorState.CREATED, 30000L)).andReturn(true);
		streamsServiceMock.start();
		control.replay();
		
		service.start();
		
		control.verify();
		assertTrue(threadSignal.await(1, TimeUnit.SECONDS));
	}
	
	@Test
	public void testStop_ShouldLogIfTimeoutWhileShuttingDown() {
		streamsServiceMock.close();
		expect(streamsServiceMock.waitForStateChangeTo(AggregatorState.DEAD, 30000L)).andReturn(false);
		// The thread did not start. It should be not alive
		control.replay();
		
		service.stop();
		
		control.verify();
	}

	@Test
	public void testStop_ShouldLogIfThreadWasNotStopped() {
		streamsServiceMock.close();
		expect(streamsServiceMock.waitForStateChangeTo(AggregatorState.DEAD, 500L)).andReturn(true);
		thread = new Thread(() -> {
				try {
					threadSignal.await(5, TimeUnit.SECONDS);
				} catch ( InterruptedException e ) { }
			}, "foobar");
		service = new RecoverableStreamsServiceStarter(thread, streamsServiceMock, 500L);
		control.replay();
		thread.start();
		
		service.stop();
		
		control.verify();
		threadSignal.countDown();
	}
	
	@Test
	public void testStop() {
		streamsServiceMock.close();
		expect(streamsServiceMock.waitForStateChangeTo(AggregatorState.DEAD, 30000L)).andReturn(true);
		control.replay();
		
		service.stop();
		
		control.verify();
	}

}
