package ru.prolib.caelum.service.aggregator.kafka;

import static org.junit.Assert.*;
import static org.easymock.EasyMock.*;

import java.time.Clock;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

import org.easymock.IMocksControl;
import org.junit.Before;
import org.junit.Test;

public class KafkaStreamsAvailabilityTest {
	IMocksControl control;
	Lock lockMock;
	Condition condMock;
	Clock clockMock;
	KafkaStreamsAvailability service;

	@Before
	public void setUp() throws Exception {
		control = createStrictControl();
		lockMock = control.createMock(Lock.class);
		condMock = control.createMock(Condition.class);
		clockMock = control.createMock(Clock.class);
		service = new KafkaStreamsAvailability(true, lockMock, condMock, clockMock);
	}
	
	@Test
	public void testCtor4() {
		assertTrue(service.isAvailable());
		assertSame(lockMock, service.getLock());
		assertSame(condMock, service.getCondition());
		assertSame(clockMock, service.getClock());
	}
	
	@Test
	public void testCtor3() {
		expect(lockMock.newCondition()).andReturn(condMock);
		control.replay();
		
		service = new KafkaStreamsAvailability(false, lockMock, clockMock);
		
		control.verify();
		assertFalse(service.isAvailable());
		assertSame(lockMock, service.getLock());
		assertSame(condMock, service.getCondition());
		assertSame(clockMock, service.getClock());
	}
	
	@Test
	public void testCtor1() {
		service = new KafkaStreamsAvailability(false);
		
		assertFalse(service.isAvailable());
		assertNotNull(service.getLock());
		assertNotNull(service.getCondition());
		assertNotNull(service.getClock());
	}
	
	@Test
	public void testCtor0() {
		service = new KafkaStreamsAvailability();
		
		assertTrue(service.isAvailable());
		assertNotNull(service.getLock());
		assertNotNull(service.getCondition());
		assertNotNull(service.getClock());
	}
	
	@Test
	public void testSetAvailable_ShouldNotLockIfAlreadyInTargetState() {
		assertTrue(service.isAvailable());
		control.replay();
		
		service.setAvailable(true);
		
		control.verify();
		assertTrue(service.isAvailable());
	}
	
	@Test
	public void testSetAvailable_ShouldChangeAndNotifyIfNotInTargetState() {
		assertTrue(service.isAvailable());
		lockMock.lock();
		condMock.signalAll();
		expectLastCall().andAnswer(() -> { assertFalse(service.isAvailable()); return null; });
		lockMock.unlock();
		control.replay();
		
		service.setAvailable(false);
		
		control.verify();
		assertFalse(service.isAvailable());
	}
	
	@Test
	public void testWaitForChange_ShouldThrowExceptionIfNegativeTimeout() {
		assertThrows(IllegalArgumentException.class, () -> { service.waitForChange(false, -1L); });
	}
	
	@Test
	public void testWaitForChange_ShouldThrowExceptionIfTimeoutIsZero() {
		assertThrows(IllegalArgumentException.class, () -> { service.waitForChange(false, 0L); });
	}
	
	@Test
	public void testWaitForChange_ShouldReturnTrueImmediatelyIfAlreadyInTargetState() {
		assertTrue(service.isAvailable());
		control.replay();
		
		assertTrue(service.waitForChange(true, 1000L));
		
		control.verify();
	}
	
	@Test
	public void testWaitForChange_ShouldReturnTrueWhenChanged() throws Exception {
		expect(clockMock.millis()).andReturn(1000L);
		lockMock.lock();
		// pass #1
		expect(condMock.await(1000L, TimeUnit.MILLISECONDS)).andReturn(true);
		expect(clockMock.millis()).andReturn(1050L);
		// pass #2
		expect(condMock.await(950L, TimeUnit.MILLISECONDS)).andReturn(true);
		expect(clockMock.millis()).andAnswer(() -> { service.setAvailable(false); return 1100L; });
		// emulate state change
		lockMock.lock();
		condMock.signalAll();
		lockMock.unlock();
		// exit waiting loop
		lockMock.unlock();
		control.replay();
		
		assertTrue(service.waitForChange(false, 1000L));
		
		control.verify();
	}
	
	@Test
	public void testWaitForChange_ShouldReturnFalseInCaseOfTimeout() throws Exception {
		expect(clockMock.millis()).andReturn(100L);
		lockMock.lock();
		// pass #1
		expect(condMock.await(100L, TimeUnit.MILLISECONDS)).andReturn(true);
		expect(clockMock.millis()).andReturn(150L);
		// pass #2
		expect(condMock.await( 50L, TimeUnit.MILLISECONDS)).andReturn(true);
		expect(clockMock.millis()).andAnswer(() -> { service.setAvailable(false); return 51L; });
		// emulate state change
		lockMock.lock();
		condMock.signalAll();
		lockMock.unlock();
		// exit waiting loop
		lockMock.unlock();
		control.replay();
		
		assertTrue(service.waitForChange(false, 100L));
		
		control.verify();
	}

}
