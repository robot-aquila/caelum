package ru.prolib.caelum.aggregator.kafka.utils;

import static org.junit.Assert.*;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.log4j.BasicConfigurator;
import org.easymock.IMocksControl;

import static org.easymock.EasyMock.*;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class RecoverableStreamsHandlerTest {
	
	@BeforeClass
	public static void setUpBeforeClass() {
		BasicConfigurator.resetConfiguration();
		BasicConfigurator.configure();
	}
	
	IMocksControl control;
	KafkaStreams streamsMock;
	IRecoverableStreamsHandlerListener listenerMock;
	AtomicInteger state;
	AtomicReference<IRecoverableStreamsHandlerListener> listenerRef;
	RecoverableStreamsHandler service;

	@Before
	public void setUp() throws Exception {
		control = createStrictControl();
		streamsMock = control.createMock(KafkaStreams.class);
		listenerMock = control.createMock(IRecoverableStreamsHandlerListener.class);
		listenerRef = new AtomicReference<>(listenerMock);
		state = new AtomicInteger();
		service = new RecoverableStreamsHandler(streamsMock, listenerRef, "foo", 5000, state);
	}
	
	@Test
	public void testCtor5() {
		assertSame(streamsMock, service.getStreams());
		assertSame(listenerMock, service.getStateListener());
		assertEquals("foo", service.getServiceName());
		assertEquals(5000L, service.getShutdownTimeout());
		assertFalse(service.started());
		assertFalse(service.recoverableError());
		assertFalse(service.unrecoverableError());
		assertFalse(service.closed());
	}
	
	@Test
	public void testCtor4() {
		service = new RecoverableStreamsHandler(streamsMock, listenerMock, "foo", 5000);
		assertSame(streamsMock, service.getStreams());
		assertSame(listenerMock, service.getStateListener());
		assertEquals("foo", service.getServiceName());
		assertEquals(5000L, service.getShutdownTimeout());
		assertFalse(service.started());
		assertFalse(service.recoverableError());
		assertFalse(service.unrecoverableError());
		assertFalse(service.closed());
	}
	
	@Test
	public void testStarted() {
		control.resetToNice();
		expect(streamsMock.close(anyObject())).andStubReturn(true);
		control.replay();
		assertFalse(service.started());

		service.onChange(KafkaStreams.State.RUNNING, null);
		assertTrue(service.started());
		
		service.close();
		assertFalse(service.started());
	}
	
	@Test
	public void testRecoverableError() {
		control.resetToNice();
		expect(streamsMock.close(anyObject())).andStubReturn(true);
		control.replay();
		
		assertFalse(service.recoverableError());
		
		service.onChange(KafkaStreams.State.RUNNING, null);
		assertFalse(service.recoverableError());
		
		service.onChange(KafkaStreams.State.ERROR, null);
		assertTrue(service.recoverableError());
		
		service.close();
		assertTrue(service.recoverableError());
	}
	
	@Test
	public void testRecoverableError_ShouldReturnFalseIfErrorAndWasNotStarted() {
		service.onChange(KafkaStreams.State.ERROR, null);
		assertFalse(service.recoverableError());
	}
	
	@Test
	public void testUnrecoverableError() {
		control.resetToNice();
		expect(streamsMock.close(anyObject())).andStubReturn(true);
		control.replay();

		assertFalse(service.unrecoverableError());
		
		service.onChange(KafkaStreams.State.ERROR, null);
		assertTrue(service.unrecoverableError());

		service.close();
		assertTrue(service.unrecoverableError());
	}
	
	@Test
	public void testUnrecoverableError_ShouldReturnFalseIfErrorAndWasStarted() {
		service.onChange(KafkaStreams.State.RUNNING, null);
		assertFalse(service.unrecoverableError());
	}
	
	@Test
	public void testClosed() {
		control.resetToNice();
		expect(streamsMock.close(anyObject())).andStubReturn(true);
		control.replay();

		assertFalse(service.closed());

		service.close();
		assertTrue(service.closed());
	}
	
	@Test
	public void testOnChange_Running_ShouldNotifyListenerAndChangeStateIfRunningFirstTime() {
		listenerMock.onStarted();
		control.replay();
		assertFalse(service.started());
		
		service.onChange(KafkaStreams.State.RUNNING, null);
		
		control.verify();
		assertTrue(service.started());
	}
	
	@Test
	public void testOnChange_Running_ShouldSkipIfRunningNextTime() {
		control.resetToNice();
		control.replay();
		service.onChange(KafkaStreams.State.RUNNING, null);
		control.resetToStrict();
		control.replay();
		assertTrue(service.started());
		
		service.onChange(KafkaStreams.State.RUNNING, null);
		
		assertTrue(service.started());
		control.verify();
	}
	
	@Test
	public void testOnChange_Running_ShouldSkipIfError() {
		control.resetToNice();
		control.replay();
		service.onChange(KafkaStreams.State.ERROR, null);
		control.resetToStrict();
		control.replay();
		assertFalse(service.started());
		
		service.onChange(KafkaStreams.State.RUNNING, null);
		
		assertFalse(service.started());
		control.verify();
	}
	
	@Test
	public void testOnChange_Error_ShouldNotifyAndChangeStateIfErrorFirstTimeAndNotStarted() {
		listenerMock.onUnrecoverableError();
		control.replay();
		assertFalse(service.started());
		assertFalse(service.recoverableError());
		assertFalse(service.unrecoverableError());
		
		service.onChange(KafkaStreams.State.ERROR, null);
		
		control.verify();
		assertFalse(service.started());
		assertFalse(service.recoverableError());
		assertTrue(service.unrecoverableError());
	}
	
	@Test
	public void testOnChange_Error_ShouldNotifyAndChangeStateIfErrorFirstTimeAndStarted() {
		control.resetToNice();
		control.replay();
		service.onChange(KafkaStreams.State.RUNNING, null);
		control.resetToStrict();
		listenerMock.onRecoverableError();
		control.replay();
		assertTrue(service.started());
		assertFalse(service.recoverableError());
		assertFalse(service.unrecoverableError());
		
		service.onChange(KafkaStreams.State.ERROR, null);
		
		control.verify();
		assertTrue(service.started());
		assertTrue(service.recoverableError());
		assertFalse(service.unrecoverableError());
	}

	@Test
	public void testOnChange_ShouldSkipIfClosed() {
		for ( KafkaStreams.State state : KafkaStreams.State.values() ) {
			control.resetToNice();
			expect(streamsMock.close(anyObject())).andStubReturn(true);
			control.replay();
			service.close();
			control.resetToStrict();
			control.replay();
			
			service.onChange(state, null);
			
			control.verify();
		}
	}
	
	private void testOnChange_SkipIf(KafkaStreams.State newState, boolean started, boolean error) {
		if ( started ) {
			control.resetToNice();
			control.replay();
			service.onChange(KafkaStreams.State.RUNNING, null);
		}
		if ( error ) {
			control.resetToNice();
			control.replay();
			service.onChange(KafkaStreams.State.ERROR, null);
		}
		control.resetToStrict();
		control.replay();
		
		service.onChange(newState, null);
		
		control.verify();
	}
	
	@Test
	public void testOnChange_ShouldSkipAnyOtherNewStateChangesInAnyState() {
		for ( KafkaStreams.State state : KafkaStreams.State.values() ) {
			switch ( state ) {
			case RUNNING:
			case ERROR:
				break;
			default:
				testOnChange_SkipIf(state, true, true);
				testOnChange_SkipIf(state, true, false);
				testOnChange_SkipIf(state, false, true);
				testOnChange_SkipIf(state, false, false);
				break;
			}
		}
	}
	
	@Test
	public void testClose_ShouldSkipIfClosed() {
		control.resetToNice();
		expect(streamsMock.close(anyObject())).andStubReturn(true);
		control.replay();
		service.close();
		control.resetToStrict();
		control.replay();
		assertTrue(service.closed());
		
		service.close();
		
		control.verify();
		assertTrue(service.closed());
	}
	
	@Test
	public void testClose_ShouldNotifyAndChangeStateIfClosedWithoutErrors() {
		expect(streamsMock.close(Duration.ofMillis(5000L))).andReturn(true);
		listenerMock.onClose(false);
		control.replay();
		assertFalse(service.closed());
		
		service.close();
		
		assertTrue(service.closed());
		control.verify();
	}
	
	@Test
	public void testClose_ShouldNotifyAndChangeStateIfClosedWithErrors() {
		expect(streamsMock.close(Duration.ofMillis(5000L))).andReturn(false);
		listenerMock.onClose(true);
		control.replay();
		assertFalse(service.closed());
		
		service.close();
		
		assertTrue(service.closed());
		control.verify();
	}
	
	@Test
	public void testStart() {
		streamsMock.setStateListener(service);
		streamsMock.cleanUp();
		streamsMock.start();
		control.replay();
		
		service.start();
		
		control.verify();
	}

}
