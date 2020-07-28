package ru.prolib.caelum.aggregator.kafka.utils;

import static org.junit.Assert.*;
import static org.easymock.EasyMock.*;
import static org.hamcrest.Matchers.*;
import static ru.prolib.caelum.aggregator.kafka.utils.RecoverableStreamsService.Mode.*;

import java.time.Clock;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

import org.apache.log4j.BasicConfigurator;
import org.easymock.IMocksControl;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import ru.prolib.caelum.aggregator.AggregatorState;
import ru.prolib.caelum.aggregator.kafka.utils.RecoverableStreamsService.StateTracker;

public class RecoverableStreamsServiceTest {
	
	@BeforeClass
	public static void setUpBeforeClass() {
		BasicConfigurator.resetConfiguration();
		BasicConfigurator.configure();
	}
	
	@Rule public ExpectedException eex = ExpectedException.none();
	IMocksControl control;
	IRecoverableStreamsController ctrlMock;
	Lock lockMock;
	Condition condMock;
	AtomicInteger totalErrors;
	Clock clockMock;
	RecoverableStreamsService service;
	StateTracker tracker, trackerMock;
	IRecoverableStreamsHandler handlerMock1, handlerMock2;

	@Before
	public void setUp() throws Exception {
		control = createStrictControl();
		ctrlMock = control.createMock(IRecoverableStreamsController.class);
		lockMock = control.createMock(Lock.class);
		condMock = control.createMock(Condition.class);
		clockMock = control.createMock(Clock.class);
		trackerMock = control.createMock(StateTracker.class);
		handlerMock1 = control.createMock(IRecoverableStreamsHandler.class);
		handlerMock2 = control.createMock(IRecoverableStreamsHandler.class);
		totalErrors = new AtomicInteger(4);
		service = new RecoverableStreamsService(ctrlMock, 100, trackerMock, totalErrors);
		tracker = new StateTracker(lockMock, condMock, clockMock);
	}
	
	@Test
	public void testStateTracker_GetState() {
		assertEquals(AggregatorState.CREATED, tracker.getState());
		
		tracker.changeState(AggregatorState.RUNNING);
		assertEquals(AggregatorState.RUNNING, tracker.getState());
		
		tracker.changeState(AggregatorState.STOPPING);
		assertEquals(AggregatorState.STOPPING, tracker.getState());
	}
	
	@Test
	public void testStateTracker_GetMode() {
		assertEquals(PENDING, tracker.getMode());
		
		tracker.changeMode(RUNNING);
		assertEquals(RUNNING, tracker.getMode());
		
		tracker.changeMode(CLOSE);
		assertEquals(CLOSE, tracker.getMode());
	}
	
	@Test
	public void testStateTracker_ChangeMode() {
		lockMock.lock();
		condMock.signalAll();
		expectLastCall().andAnswer(() -> { assertThat(tracker.getMode(), is(equalTo(CLOSE))); return null; });
		lockMock.unlock();
		control.replay();
		
		tracker.changeMode(CLOSE);
	}
	
	@Test
	public void testStateTracker_ChangeModeIf_ShouldChangeIfExpectationIsSatisfied() {
		lockMock.lock();
		condMock.signalAll();
		expectLastCall().andAnswer(() -> { assertThat(tracker.getMode(), is(equalTo(RUNNING))); return null; });
		lockMock.unlock();
		control.replay();
		
		assertTrue(tracker.changeModeIf(PENDING, RUNNING));
		
		control.verify();
		
	}
	
	@Test
	public void testStateTracker_ChangeModeIf_ShouldSkipIfExpectationIsNotSatisfied() {
		control.replay();
		
		assertFalse(tracker.changeModeIf(RUNNING, PENDING));
		
		control.verify();
	}
	
	@Test
	public void testStateTracker_ChangeModeIfNot_ShouldChangeIfExpectationIsSatisfied() {
		lockMock.lock();
		condMock.signalAll();
		expectLastCall().andAnswer(() -> { assertThat(tracker.getMode(), is(equalTo(RUNNING))); return null; });
		lockMock.unlock();
		control.replay();
		
		assertTrue(tracker.changeModeIfNot(CLOSE, RUNNING));
		
		control.verify();
	}
	
	@Test
	public void testStateTracker_ChangeModeIfNot_ShouldSkipIfExpectationIsNotSatisfied() {
		control.replay();
		
		assertFalse(tracker.changeModeIfNot(PENDING, RUNNING));
		
		control.verify();
	}
	
	@Test
	public void testStateTracker_ChangeState() {
		lockMock.lock();
		condMock.signalAll();
		expectLastCall().andAnswer(() ->
			{ assertThat(tracker.getState(), is(equalTo(AggregatorState.ERROR))); return null; });
		lockMock.unlock();
		control.replay();
		
		tracker.changeState(AggregatorState.ERROR);
		
		control.verify();
	}
	
	@Test
	public void testStateTracker_ChangeStateIf_ShouldChangeIsExpectationIsSatisfied() {
		lockMock.lock();
		condMock.signalAll();
		expectLastCall().andAnswer(() ->
			{ assertThat(tracker.getState(), is(equalTo(AggregatorState.DEAD))); return null; });
		lockMock.unlock();
		control.replay();
		
		assertTrue(tracker.changeStateIf(AggregatorState.CREATED, AggregatorState.DEAD));
		
		control.verify();
	}
	
	@Test
	public void testStateTracker_ChangeStateIf_ShouldSkipIfExpectationIsNotSatisfied() {
		control.replay();
		
		assertFalse(tracker.changeStateIf(AggregatorState.ERROR, AggregatorState.RUNNING));
		
		control.verify();
	}
	
	@Test
	public void testStateTracker_NotifyChanged() {
		lockMock.lock();
		condMock.signalAll();
		lockMock.unlock();
		control.replay();
		
		tracker.notifyChanged();
		
		control.verify();
	}
	
	@Test
	public void testStateTracker_WaitForChanges() throws Exception {
		lockMock.lock();
		expect(condMock.await(1, TimeUnit.SECONDS)).andReturn(false); // the result does not matter
		lockMock.unlock();
		control.replay();
		
		tracker.waitForChanges();
		
		control.verify();
	}
	
	@Test
	public void testStateTracker_WaitForStateChange_ShouldReturnImmediatelyIfCurrentStateIsExpected() {
		tracker.changeState(AggregatorState.ERROR);
		control.resetToStrict();
		control.replay();
		
		assertTrue(tracker.waitForStateChange(AggregatorState.ERROR, 1000L));
		
		control.verify();
	}
	
	@Test
	public void testStateTracker_WaitForStateChange_ShouldReturnTrueIfStateChanged() throws Exception {
		assertEquals(AggregatorState.CREATED, tracker.getState());
		expect(clockMock.millis()).andReturn(100L);
		lockMock.lock(); // start
		// pass #1
		expect(condMock.await(500, TimeUnit.MILLISECONDS)).andReturn(true);
		expect(clockMock.millis()).andReturn(200L);
		// pass #2
		expect(condMock.await(400, TimeUnit.MILLISECONDS)).andReturn(true);
		expect(clockMock.millis()).andAnswer(() -> { tracker.changeState(AggregatorState.ERROR); return 300L; });
		// # changing state
		lockMock.lock();
		condMock.signalAll();
		lockMock.unlock();
		lockMock.unlock(); // end
		control.replay();
		
		assertTrue(tracker.waitForStateChange(AggregatorState.ERROR, 500L));
		
		control.verify();
		assertEquals(AggregatorState.ERROR, tracker.getState());
	}
	
	@Test
	public void testStateTracker_WaitForStateChange_ShouldReturnFalseIfStateNotChanged() throws Exception {
		assertEquals(AggregatorState.CREATED, tracker.getState());
		expect(clockMock.millis()).andReturn(100L);
		lockMock.lock(); // start
		// pass #1
		expect(condMock.await(200, TimeUnit.MILLISECONDS)).andReturn(true);
		expect(clockMock.millis()).andReturn(200L);
		// pass #2
		expect(condMock.await(100, TimeUnit.MILLISECONDS)).andReturn(true);
		expect(clockMock.millis()).andReturn(301L);
		lockMock.unlock(); // end
		control.replay();
		
		assertFalse(tracker.waitForStateChange(AggregatorState.ERROR, 200L));

		control.verify();
		assertEquals(AggregatorState.CREATED, tracker.getState());
	}
	
	@Test
	public void testStateTracker_WaitForStateChangeFrom_ShouldReturnImmediatelyIfCurrentStateIsNotExpected() {
		assertEquals(AggregatorState.CREATED, tracker.getState());
		control.replay();
		
		assertTrue(tracker.waitForStateChangeFrom(AggregatorState.RUNNING, 200L));
		
		control.verify();
	}
	
	@Test
	public void testStateTracker_WaitForStateChangeFrom_ShouldReturnTrueIfStateChanged() throws Exception {
		assertEquals(AggregatorState.CREATED, tracker.getState());
		expect(clockMock.millis()).andReturn(100L);
		lockMock.lock(); // start
		// pass #1
		expect(condMock.await(500, TimeUnit.MILLISECONDS)).andReturn(true);
		expect(clockMock.millis()).andReturn(200L);
		// pass #2
		expect(condMock.await(400, TimeUnit.MILLISECONDS)).andReturn(true);
		expect(clockMock.millis()).andAnswer(() -> { tracker.changeState(AggregatorState.ERROR); return 300L; });
		// # changing state
		lockMock.lock();
		condMock.signalAll();
		lockMock.unlock();
		lockMock.unlock(); // end
		control.replay();
		
		assertTrue(tracker.waitForStateChangeFrom(AggregatorState.CREATED, 500L));
		
		control.verify();
		assertEquals(AggregatorState.ERROR, tracker.getState());
	}
	
	@Test
	public void testStateTracker_WaitForStateChangeFrom_ShouldReturnFalseIfStateNotChanged() throws Exception {
		assertEquals(AggregatorState.CREATED, tracker.getState());
		expect(clockMock.millis()).andReturn(100L);
		lockMock.lock(); // start
		// pass #1
		expect(condMock.await(200, TimeUnit.MILLISECONDS)).andReturn(true);
		expect(clockMock.millis()).andReturn(200L);
		// pass #2
		expect(condMock.await(100, TimeUnit.MILLISECONDS)).andReturn(true);
		expect(clockMock.millis()).andReturn(301L);
		lockMock.unlock(); // end
		control.replay();
		
		assertFalse(tracker.waitForStateChangeFrom(AggregatorState.CREATED, 200L));
		
		control.verify();
		assertEquals(AggregatorState.CREATED, tracker.getState());
	}
	
	static class TriggerThread extends Thread {
		final CountDownLatch started = new CountDownLatch(1);
		final CountDownLatch finished = new CountDownLatch(1);
		final CountDownLatch trigger;
		final Runnable action;
		
		public TriggerThread(CountDownLatch trigger, Runnable action) {
			super();
			setDaemon(true);
			this.trigger = (trigger == null ? new CountDownLatch(1) : trigger);
			this.action = action;
		}
		
		@Override
		public void run() {
			started.countDown();
			try {
				trigger.await();
				action.run();
				finished.countDown();
			} catch ( Throwable t ) {
				System.err.println(t.getMessage());
				t.printStackTrace(System.err);
			}
		}

	}
	
	private void testStateTracker_WaitForStateChange3_TestHelper(Runnable actor, Runnable checker) throws Exception {
		TriggerThread checker_thread = new TriggerThread(null, checker), actor_thread = new TriggerThread(null, actor);
		actor_thread.start();
		checker_thread.start();
		assertTrue(actor_thread.started.await(1, TimeUnit.SECONDS));
		assertTrue(checker_thread.started.await(1, TimeUnit.SECONDS));
		checker_thread.trigger.countDown();
		actor_thread.trigger.countDown();
		assertTrue(actor_thread.finished.await(1, TimeUnit.SECONDS));
		assertTrue(checker_thread.finished.await(1, TimeUnit.SECONDS));
	}
	
	@Test
	public void testStateTracker_WaitForStateChange3() throws Exception {
		Collection<AggregatorState>
			createdOrPending = Arrays.asList(AggregatorState.CREATED, AggregatorState.PENDING),
			startingOrRunning = Arrays.asList(AggregatorState.STARTING, AggregatorState.RUNNING);
		tracker = new StateTracker();
		
		tracker.changeState(AggregatorState.ERROR);
		assertFalse(tracker.waitForStateChange(createdOrPending, startingOrRunning, 250L));
		
		tracker.changeState(AggregatorState.STARTING);
		assertTrue(tracker.waitForStateChange(createdOrPending, startingOrRunning, 250L));
		
		tracker.changeState(AggregatorState.RUNNING);
		assertTrue(tracker.waitForStateChange(createdOrPending, startingOrRunning, 250L));
		
		tracker.changeState(AggregatorState.CREATED);
		testStateTracker_WaitForStateChange3_TestHelper(
				() -> tracker.changeState(AggregatorState.STARTING),
				() -> assertTrue(tracker.waitForStateChange(createdOrPending, startingOrRunning, 250L))
			);
		
		tracker.changeState(AggregatorState.CREATED);
		testStateTracker_WaitForStateChange3_TestHelper(
				() -> tracker.changeState(AggregatorState.RUNNING),
				() -> assertTrue(tracker.waitForStateChange(createdOrPending, startingOrRunning, 250L))
			);
		
		tracker.changeState(AggregatorState.CREATED);
		testStateTracker_WaitForStateChange3_TestHelper(
				() -> tracker.changeState(AggregatorState.ERROR),
				() -> assertFalse(tracker.waitForStateChange(createdOrPending, startingOrRunning, 250L))
			);
		
		tracker.changeState(AggregatorState.CREATED);
		testStateTracker_WaitForStateChange3_TestHelper(
				() -> { /* do nothing */ },
				() -> assertFalse(tracker.waitForStateChange(createdOrPending, startingOrRunning, 250L))
			);

		tracker.changeState(AggregatorState.PENDING);
		testStateTracker_WaitForStateChange3_TestHelper(
				() -> tracker.changeState(AggregatorState.STARTING),
				() -> assertTrue(tracker.waitForStateChange(createdOrPending, startingOrRunning, 250L))
			);
		
		tracker.changeState(AggregatorState.PENDING);
		testStateTracker_WaitForStateChange3_TestHelper(
				() -> tracker.changeState(AggregatorState.RUNNING),
				() -> assertTrue(tracker.waitForStateChange(createdOrPending, startingOrRunning, 250L))
			);
		
		tracker.changeState(AggregatorState.PENDING);
		testStateTracker_WaitForStateChange3_TestHelper(
				() -> tracker.changeState(AggregatorState.ERROR),
				() -> assertFalse(tracker.waitForStateChange(createdOrPending, startingOrRunning, 250L))
			);

		tracker.changeState(AggregatorState.PENDING);
		testStateTracker_WaitForStateChange3_TestHelper(
				() -> { /* do nothing */ },
				() -> assertFalse(tracker.waitForStateChange(createdOrPending, startingOrRunning, 250L))
			);

	}
	
	@Test
	public void testCtor4() {
		assertSame(ctrlMock, service.getController());
		assertEquals(100, service.getMaxErrors());
		assertSame(trackerMock, service.getStateTracker());
		assertEquals(4, service.getTotalErrors());
	}
	
	@Test
	public void testCtor2() {
		control.replay();
		
		service = new RecoverableStreamsService(ctrlMock, 300);
		
		control.verify();
		assertSame(ctrlMock, service.getController());
		assertEquals(300, service.getMaxErrors());
		assertEquals(0, service.getTotalErrors());
		assertNotNull(service.getStateTracker());
	}
	
	@Test
	public void testGetState() {
		expect(trackerMock.getState()).andReturn(AggregatorState.RUNNING);
		control.replay();
		
		assertEquals(AggregatorState.RUNNING, service.getState());
		
		control.verify();
	}
	
	@Test
	public void testGetMode() {
		expect(trackerMock.getMode()).andReturn(RUNNING);
		control.replay();
		
		assertEquals(RUNNING, service.getMode());
		
		control.verify();
	}
	
	@Test
	public void testStart_ShouldStartIfPending() {
		expect(trackerMock.getMode()).andReturn(PENDING);
		expect(trackerMock.changeModeIf(PENDING, RUNNING)).andReturn(true);
		control.replay();
		
		service.start();
		
		control.verify();
	}
	
	@Test
	public void testStart_ShouldThrowIfClosed() {
		expect(trackerMock.getMode()).andReturn(CLOSE);
		control.replay();
		eex.expect(IllegalStateException.class);
		eex.expectMessage("Service closed");
		
		service.start();
	}
	
	@Test
	public void testStart_ShouldSkipIfStarted() {
		expect(trackerMock.getMode()).andReturn(RUNNING);
		control.replay();
		
		service.start();
		
		control.verify();
	}
	
	@Test
	public void testStartAndWaitConfirm_ShouldStartIfPending() {
		expect(trackerMock.getMode()).andReturn(PENDING);
		expect(trackerMock.changeModeIf(PENDING, RUNNING)).andReturn(true);
		expect(trackerMock.waitForStateChange(
				Arrays.asList(AggregatorState.CREATED, AggregatorState.PENDING),
				Arrays.asList(AggregatorState.RUNNING, AggregatorState.STARTING, AggregatorState.ERROR),
				2500L)).andReturn(false);
		control.replay();

		assertFalse(service.startAndWaitConfirm(2500L));
		
		control.verify();
	}

	@Test
	public void testStartAndWaitConfirm_ShouldThrowsIfClosed() {
		expect(trackerMock.getMode()).andReturn(CLOSE);
		control.replay();
		eex.expect(IllegalStateException.class);
		eex.expectMessage("Service closed");
		
		service.startAndWaitConfirm(1000L);
	}
	
	@Test
	public void testStartAndWaitConfirm_ShouldSkipIfStarted() {
		expect(trackerMock.getMode()).andReturn(RUNNING);
		expect(trackerMock.waitForStateChange(
				Arrays.asList(AggregatorState.CREATED, AggregatorState.PENDING),
				Arrays.asList(AggregatorState.RUNNING, AggregatorState.STARTING, AggregatorState.ERROR),
				1000L)).andReturn(true);
		control.replay();

		assertTrue(service.startAndWaitConfirm(1000L));
		
		control.verify();
	}
	
	@Test
	public void testStop_ShouldStopIfStarted() {
		expect(trackerMock.getMode()).andReturn(RUNNING);
		expect(trackerMock.changeModeIf(RUNNING, PENDING)).andReturn(true);
		control.replay();
		
		service.stop();
		
		control.verify();
	}
	
	@Test
	public void testStop_ShouldThrowIfClosed() {
		expect(trackerMock.getMode()).andReturn(CLOSE);
		control.replay();
		eex.expect(IllegalStateException.class);
		eex.expectMessage("Service closed");
		
		service.stop();
	}
	
	@Test
	public void testStop_ShouldSkipIfPending() {
		expect(trackerMock.getMode()).andReturn(PENDING);
		control.replay();
		
		service.stop();
		
		control.verify();
	}
	
	@Test
	public void testStopAndWaitConfirm_ShouldStopIfStarted() {
		expect(trackerMock.getMode()).andReturn(RUNNING);
		expect(trackerMock.changeModeIf(RUNNING, PENDING)).andReturn(true);
		expect(trackerMock.waitForStateChange(AggregatorState.PENDING, 250L)).andReturn(true);
		control.replay();
		
		assertTrue(service.stopAndWaitConfirm(250L));
		
		control.verify();
	}
	
	@Test
	public void testStopAndWaitConfirm_ShouldThrowsIfClosed() {
		expect(trackerMock.getMode()).andReturn(CLOSE);
		control.replay();
		eex.expect(IllegalStateException.class);
		eex.expectMessage("Service closed");

		service.stopAndWaitConfirm(1000L);
	}
	
	@Test
	public void testStopAndWaitConfirm_ShouldSkipIfPending() {
		expect(trackerMock.getMode()).andReturn(PENDING);
		expect(trackerMock.waitForStateChange(AggregatorState.PENDING, 250L)).andReturn(false);
		control.replay();
		
		assertFalse(service.stopAndWaitConfirm(250L));
		
		control.verify();
	}
	
	@Test
	public void testClose_ShouldCloseIfStarted() {
		expect(trackerMock.getMode()).andReturn(RUNNING);
		expect(trackerMock.changeModeIfNot(CLOSE, CLOSE)).andReturn(true);
		control.replay();
		
		service.close();
		
		control.verify();
	}
	
	@Test
	public void testClose_ShouldCloseIfPending() {
		expect(trackerMock.getMode()).andReturn(PENDING);
		expect(trackerMock.changeModeIfNot(CLOSE, CLOSE)).andReturn(true);
		control.replay();
		
		service.close();
		
		control.verify();
	}
	
	@Test
	public void testClose_ShouldSkipIfClosed() {
		expect(trackerMock.getMode()).andReturn(CLOSE);
		control.replay();
		
		service.close();
		
		control.verify();
	}
	
	@Test
	public void testOnStarted() {
		trackerMock.notifyChanged();
		control.replay();
		
		service.onStarted();
		
		control.verify();
	}
	
	@Test
	public void testOnRecoverableError() {
		trackerMock.notifyChanged();
		control.replay();
		
		service.onRecoverableError();
		
		control.verify();
		assertEquals(5, totalErrors.get());
	}
	
	@Test
	public void testOnUnrecoverableError() {
		trackerMock.notifyChanged();
		control.replay();
		
		service.onUnrecoverableError();
		
		control.verify();
		assertEquals(5, totalErrors.get());
	}
	
	@Test
	public void testOnClose() {
		control.replay();
		
		service.onClose(false);
		
		control.verify();
		assertEquals(4, totalErrors.get());
	}
	
	@Test
	public void testOnClose_ShouldIncreaseErrorCounterIfError() {
		control.replay();
		
		service.onClose(true);
		
		control.verify();
		assertEquals(5, totalErrors.get());
	}
	
	@Test
	public void testRun_ShouldSwitchFromPendingToClose() {
		trackerMock.changeState(AggregatorState.PENDING);
		expect(trackerMock.getMode()).andReturn(CLOSE);
		trackerMock.changeState(AggregatorState.DEAD);

		control.replay();
		
		service.run();
		
		control.verify();
	}
	
	@Test
	public void testRun_ShouldKeepPending_WaitForChanges() {
		trackerMock.changeState(AggregatorState.PENDING);
		expect(trackerMock.getMode()).andReturn(PENDING);
		trackerMock.waitForChanges();
		expect(trackerMock.getMode()).andReturn(CLOSE);
		trackerMock.changeState(AggregatorState.DEAD);
		control.replay();
		
		service.run();
		
		control.verify();
	}

	@Test
	public void testRun_ShouldSwitchFromPendingToRunning() {
		totalErrors.set(524);
		trackerMock.changeState(AggregatorState.PENDING);
		// expect switch to running
		expect(trackerMock.getMode()).andReturn(RUNNING);
		trackerMock.changeState(AggregatorState.STARTING);
		expectLastCall().andAnswer(() -> { assertEquals(0, totalErrors.get()); return null; });
		expect(ctrlMock.build(service)).andReturn(handlerMock1);
		handlerMock1.start();
		// expect close from running
		expect(trackerMock.getMode()).andReturn(CLOSE);
		ctrlMock.onClose(handlerMock1);
		handlerMock1.close();
		trackerMock.changeState(AggregatorState.DEAD);
		control.replay();

		service.run();
		
		control.verify();
	}
	
	@Test
	public void testRun_ShouldSwitchFromRunningToPending() {
		trackerMock.changeState(AggregatorState.PENDING);
		// expect go to running first
		expect(trackerMock.getMode()).andReturn(RUNNING);
		trackerMock.changeState(AggregatorState.STARTING);
		expect(ctrlMock.build(service)).andReturn(handlerMock1);
		handlerMock1.start();
		// expect switch to pending
		expect(trackerMock.getMode()).andReturn(PENDING);
		ctrlMock.onClose(handlerMock1);
		handlerMock1.close();
		trackerMock.changeState(AggregatorState.PENDING);
		// expect close from pending
		expect(trackerMock.getMode()).andReturn(CLOSE);
		trackerMock.changeState(AggregatorState.DEAD);
		control.replay();

		service.run();
		
		control.verify();
	}
	
	@Test
	public void testRun_ShouldSwitchFromRunningToClose() {
		trackerMock.changeState(AggregatorState.PENDING);
		// expect go to running first
		expect(trackerMock.getMode()).andReturn(RUNNING);
		trackerMock.changeState(AggregatorState.STARTING);
		expect(ctrlMock.build(service)).andReturn(handlerMock1);
		handlerMock1.start();
		// expect switch to close
		expect(trackerMock.getMode()).andReturn(CLOSE);
		ctrlMock.onClose(handlerMock1);
		handlerMock1.close();
		trackerMock.changeState(AggregatorState.DEAD);
		control.replay();

		service.run();
		
		control.verify();
	}
	
	@Test
	public void testRun_ShouldSwitchFromRunningToPending_IfHandlerNotCreated() {
		trackerMock.changeState(AggregatorState.PENDING);
		// switch to running with handler failure
		expect(trackerMock.getMode()).andReturn(RUNNING);
		trackerMock.changeState(AggregatorState.STARTING);
		expect(ctrlMock.build(service)).andThrow(new RuntimeException("Test error"));
		trackerMock.changeState(AggregatorState.ERROR);
		// expect switch to close
		expect(trackerMock.getMode()).andReturn(PENDING);
		trackerMock.changeState(AggregatorState.PENDING);
		// expect close from pending
		expect(trackerMock.getMode()).andReturn(CLOSE);
		trackerMock.changeState(AggregatorState.DEAD);
		control.replay();
		
		service.run();
		
		control.verify();
	}
	
	@Test
	public void testRun_ShouldSwitchFromRunningToClose_IfHandlerNotCreated() {
		trackerMock.changeState(AggregatorState.PENDING);
		expect(trackerMock.getMode()).andReturn(CLOSE);
		trackerMock.changeState(AggregatorState.DEAD);
		control.replay();
		
		service.run();
		
		control.verify();
	}
	
	@Test
	public void testRun_ShouldSwitchFromRunningToPending_CloseHandlerNicelyEvenIfErrorsOnClosing() {
		trackerMock.changeState(AggregatorState.PENDING);
		// expect go to running first
		expect(trackerMock.getMode()).andReturn(RUNNING);
		trackerMock.changeState(AggregatorState.STARTING);
		expect(ctrlMock.build(service)).andReturn(handlerMock1);
		handlerMock1.start();
		// expect switch to pending
		expect(trackerMock.getMode()).andReturn(PENDING);
		ctrlMock.onClose(handlerMock1);
		handlerMock1.close();
		expectLastCall().andThrow(new RuntimeException("Test error"));
		trackerMock.changeState(AggregatorState.PENDING);
		// expect close from pending
		expect(trackerMock.getMode()).andReturn(CLOSE);
		trackerMock.changeState(AggregatorState.DEAD);
		control.replay();

		service.run();
		
		control.verify();
	}
	
	@Test
	public void testRun_ShouldSwitchFromRunningToClose_CloseHandlerNicelyEvenIfErrorsOnClosing() {
		trackerMock.changeState(AggregatorState.PENDING);
		// expect go to running first
		expect(trackerMock.getMode()).andReturn(RUNNING);
		trackerMock.changeState(AggregatorState.STARTING);
		expect(ctrlMock.build(service)).andReturn(handlerMock1);
		handlerMock1.start();
		// expect switch to close
		expect(trackerMock.getMode()).andReturn(CLOSE);
		ctrlMock.onClose(handlerMock1);
		handlerMock1.close();
		expectLastCall().andThrow(new RuntimeException("Test error"));
		trackerMock.changeState(AggregatorState.DEAD);
		control.replay();

		service.run();
		
		control.verify();
	}
	
	@Test
	public void testRun_ShouldKeepRunningAtUnrecoverableError_DoNotCreateMoreHandlers() {
		trackerMock.changeState(AggregatorState.PENDING);
		// expect go to running first
		expect(trackerMock.getMode()).andReturn(RUNNING);
		trackerMock.changeState(AggregatorState.STARTING);
		expect(ctrlMock.build(service)).andReturn(handlerMock1);
		handlerMock1.start();
		// expect to get unrecoverable error
		expect(trackerMock.getMode()).andReturn(RUNNING);
		expect(handlerMock1.unrecoverableError()).andReturn(true);
		ctrlMock.onClose(handlerMock1);
		handlerMock1.close();
		trackerMock.changeState(AggregatorState.ERROR);
		// next pass should cause wait for changes
		expect(trackerMock.getMode()).andReturn(RUNNING);
		trackerMock.waitForChanges();
		// expect close
		expect(trackerMock.getMode()).andReturn(CLOSE);
		trackerMock.changeState(AggregatorState.DEAD);
		control.replay();

		service.run();
		
		control.verify();
	}
	
	@Test
	public void testRun_ShouldKeepRunningAtRecoverableError_ReplaceHandlerIfNotManyErrors() {
		trackerMock.changeState(AggregatorState.PENDING);
		// expect go to running first
		expect(trackerMock.getMode()).andReturn(RUNNING);
		trackerMock.changeState(AggregatorState.STARTING);
		expect(ctrlMock.build(service)).andReturn(handlerMock1);
		handlerMock1.start();
		// expect to get recoverable error
		expect(trackerMock.getMode()).andReturn(RUNNING);
		expect(handlerMock1.unrecoverableError()).andReturn(false);
		expect(handlerMock1.recoverableError()).andReturn(true);
		ctrlMock.onClose(handlerMock1);
		handlerMock1.close();
		trackerMock.changeState(AggregatorState.STARTING);
		expect(ctrlMock.build(service)).andReturn(handlerMock2);
		handlerMock2.start();
		// expect close
		expect(trackerMock.getMode()).andReturn(CLOSE);
		ctrlMock.onClose(handlerMock2);
		handlerMock2.close();
		trackerMock.changeState(AggregatorState.DEAD);
		control.replay();

		service.run();
		
		control.verify();
	}
	
	@Test
	public void testRun_ShouldKeepRunningAtRecoverableError_DoNotCreateMoreHandlersIfTooManyErrors() {
		// The errors counter will reset when go to running.
		// Do not set up it here before get into running mode.
		trackerMock.changeState(AggregatorState.PENDING);
		// expect go to running first
		expect(trackerMock.getMode()).andReturn(RUNNING);
		trackerMock.changeState(AggregatorState.STARTING);
		expectLastCall().andAnswer(() -> { totalErrors.set(100); return null; });
		expect(ctrlMock.build(service)).andReturn(handlerMock1);
		handlerMock1.start();
		// expect to get recoverable error
		expect(trackerMock.getMode()).andReturn(RUNNING);
		expect(handlerMock1.unrecoverableError()).andReturn(false);
		expect(handlerMock1.recoverableError()).andReturn(true);
		ctrlMock.onClose(handlerMock1);
		handlerMock1.close();
		trackerMock.changeState(AggregatorState.ERROR);
		// next pass should cause wait for changes
		expect(trackerMock.getMode()).andReturn(RUNNING);
		trackerMock.waitForChanges();
		// expect close
		expect(trackerMock.getMode()).andReturn(CLOSE);
		trackerMock.changeState(AggregatorState.DEAD);
		control.replay();

		service.run();
		
		control.verify();
	}
	
	@Test
	public void testRun_ShouldKeepRunningIfHandlerStartedAndStateIsStarting_SetRunningState() {
		trackerMock.changeState(AggregatorState.PENDING);
		// expect go to running first
		expect(trackerMock.getMode()).andReturn(RUNNING);
		trackerMock.changeState(AggregatorState.STARTING);
		expect(ctrlMock.build(service)).andReturn(handlerMock1);
		handlerMock1.start();
		// expect to get starting notification
		expect(trackerMock.getMode()).andReturn(RUNNING);
		expect(handlerMock1.unrecoverableError()).andReturn(false);
		expect(handlerMock1.recoverableError()).andReturn(false);
		expect(handlerMock1.started()).andReturn(true);
		expect(trackerMock.changeStateIf(AggregatorState.STARTING, AggregatorState.RUNNING)).andReturn(true);
		ctrlMock.onRunning(handlerMock1);
		// expect close
		expect(trackerMock.getMode()).andReturn(CLOSE);
		ctrlMock.onClose(handlerMock1);
		handlerMock1.close();
		trackerMock.changeState(AggregatorState.DEAD);
		control.replay();

		service.run();
		
		control.verify();
	}
	
	@Test
	public void testRun_ShouldKeepRunningIfHandlerStartedButStateIsNotStarting_WaitForChanges() {
		trackerMock.changeState(AggregatorState.PENDING);
		// expect go to running first
		expect(trackerMock.getMode()).andReturn(RUNNING);
		trackerMock.changeState(AggregatorState.STARTING);
		expect(ctrlMock.build(service)).andReturn(handlerMock1);
		handlerMock1.start();
		// expect to get starting notification
		expect(trackerMock.getMode()).andReturn(RUNNING);
		expect(handlerMock1.unrecoverableError()).andReturn(false);
		expect(handlerMock1.recoverableError()).andReturn(false);
		expect(handlerMock1.started()).andReturn(true);
		expect(trackerMock.changeStateIf(AggregatorState.STARTING, AggregatorState.RUNNING)).andReturn(false);
		trackerMock.waitForChanges();
		// expect close
		expect(trackerMock.getMode()).andReturn(CLOSE);
		ctrlMock.onClose(handlerMock1);
		handlerMock1.close();
		trackerMock.changeState(AggregatorState.DEAD);
		control.replay();

		service.run();
		
		control.verify();
	}
	
	@Test
	public void testRun_ShouldKeepRunningIfDoNotKnowWhatToDo_WaitForChanges() {
		trackerMock.changeState(AggregatorState.PENDING);
		// expect go to running first
		expect(trackerMock.getMode()).andReturn(RUNNING);
		trackerMock.changeState(AggregatorState.STARTING);
		expect(ctrlMock.build(service)).andReturn(handlerMock1);
		handlerMock1.start();
		// expect to get starting notification
		expect(trackerMock.getMode()).andReturn(RUNNING);
		expect(handlerMock1.unrecoverableError()).andReturn(false);
		expect(handlerMock1.recoverableError()).andReturn(false);
		expect(handlerMock1.started()).andReturn(false);
		trackerMock.waitForChanges();
		// expect close
		expect(trackerMock.getMode()).andReturn(CLOSE);
		ctrlMock.onClose(handlerMock1);
		handlerMock1.close();
		trackerMock.changeState(AggregatorState.DEAD);
		control.replay();

		service.run();
		
		control.verify();
	}
	
	@Test
	public void testRun_ShouldKeepRunningIfHandlerCreationFailed_GetStuckInRunning() {
		trackerMock.changeState(AggregatorState.PENDING);
		// expect go to running first
		expect(trackerMock.getMode()).andReturn(RUNNING);
		trackerMock.changeState(AggregatorState.STARTING);
		expect(ctrlMock.build(service)).andThrow(new RuntimeException("Test error"));
		trackerMock.changeState(AggregatorState.ERROR);
		// next pass should cause wait for changes
		expect(trackerMock.getMode()).andReturn(RUNNING);
		trackerMock.waitForChanges();
		// expect close
		expect(trackerMock.getMode()).andReturn(CLOSE);
		trackerMock.changeState(AggregatorState.DEAD);
		control.replay();

		service.run();
		
		control.verify();
	}
	
	@Test
	public void testRun_ShouldKeepRunningIfHandlerStartupFailed_GetStuckInRunning() {
		trackerMock.changeState(AggregatorState.PENDING);
		// expect go to running first
		expect(trackerMock.getMode()).andReturn(RUNNING);
		trackerMock.changeState(AggregatorState.STARTING);
		expect(ctrlMock.build(service)).andReturn(handlerMock1);
		trackerMock.changeState(AggregatorState.ERROR);
		handlerMock1.close();
		// next pass should cause wait for changes
		expect(trackerMock.getMode()).andReturn(RUNNING);
		trackerMock.waitForChanges();
		// expect close
		expect(trackerMock.getMode()).andReturn(CLOSE);
		trackerMock.changeState(AggregatorState.DEAD);
		control.replay();

		service.run();
		
		control.verify();
	}
	
	@Test
	public void testWaitForStateChangeTo() {
		expect(trackerMock.waitForStateChange(AggregatorState.ERROR, 1000L)).andReturn(true).andReturn(false);
		control.replay();
		
		assertTrue(service.waitForStateChangeTo(AggregatorState.ERROR, 1000L));
		assertFalse(service.waitForStateChangeTo(AggregatorState.ERROR, 1000L));
		
		control.verify();
	}
	
	@Test
	public void testWaitForStateChangeFrom() {
		expect(trackerMock.waitForStateChangeFrom(AggregatorState.ERROR, 2000L)).andReturn(false).andReturn(true);
		control.replay();
		
		assertFalse(service.waitForStateChangeFrom(AggregatorState.ERROR, 2000L));
		assertTrue(service.waitForStateChangeFrom(AggregatorState.ERROR, 2000L));
		
		control.verify();
	}
	
}
