package ru.prolib.caelum.aggregator.kafka.utils;

import java.time.Clock;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.prolib.caelum.aggregator.AggregatorState;
import ru.prolib.caelum.core.ServiceException;

public class RecoverableStreamsService implements IRecoverableStreamsService,
	IRecoverableStreamsHandlerListener, Runnable
{
	private static final Logger logger = LoggerFactory.getLogger(RecoverableStreamsService.class);
	
	public enum Mode {
		PENDING,
		RUNNING,
		CLOSE
	}
	
	static class StateTracker {
		private final Lock stateLock;
		private final Condition stateChanged;
		private final Clock clock;
		private volatile AggregatorState state;
		private volatile Mode mode;
		
		StateTracker(Lock stateLock, Condition stateChanged, Clock clock) {
			this.stateLock = stateLock;
			this.stateChanged = stateChanged;
			this.clock = clock;
			this.state = AggregatorState.CREATED;
			this.mode = Mode.PENDING;
		}
		
		public StateTracker(Lock stateLock) {
			this(stateLock, stateLock.newCondition(), Clock.systemUTC());
		}
		
		public StateTracker() {
			this(new ReentrantLock());
		}
		
		/**
		 * Get current state.
		 * <p>
		 * @return state
		 */
		public AggregatorState getState() {
			return state;
		}
		
		/**
		 * Get current mode.
		 * <p>
		 * @return mode
		 */
		public Mode getMode() {
			return mode;
		}
		
		/**
		 * Set new mode and notify.
		 * <p>
		 * @param new_mode - new mode
		 */
		public void changeMode(Mode new_mode) {
			stateLock.lock();
			try {
				logger.debug("Mode changed: {} -> {}", mode, new_mode);
				mode = new_mode;
				stateChanged.signalAll();
			} finally {
				stateLock.unlock();
			}
		}
				
		/**
		 * Change mode if current mode equal to expected.
		 * <p>
		 * @param expected - expected current mode
		 * @param new_mode - new mode to set if condition is met
		 * @return true if mode has changed, false otherwise
		 */
		public boolean changeModeIf(Mode expected, Mode new_mode) {
			if ( mode != expected ) {
				return false;
			}
			stateLock.lock();
			try {
				if ( mode == expected ) {
					logger.debug("Mode changed: {} -> {}", mode, new_mode);
					mode = new_mode;
					stateChanged.signalAll();
					return true;
				} else {
					return false;
				}
			} finally {
				stateLock.unlock();
			}
		}
		
		
		/**
		 * Change mode if current mode not equal to expected.
		 * <p>
		 * @param not_expected - mode that expected to not be current
		 * @param new_mode - new mode to set if condition is met
		 * @return true if mode has changed, false otherwise
		 */
		public boolean changeModeIfNot(Mode not_expected, Mode new_mode) {
			if ( mode == not_expected ) {
				return false;
			}
			stateLock.lock();
			try {
				if ( mode != not_expected ) {
					logger.debug("Mode changed: {} -> {}", mode, new_mode);
					mode = new_mode;
					stateChanged.signalAll();
					return true;
				} else {
					return false;
				}
			} finally {
				stateLock.unlock();
			}
		}
		
		/**
		 * Set new state and notify.
		 * <p>
		 * @param new_state - new state
		 */
		public void changeState(AggregatorState new_state) {
			stateLock.lock();
			try {
				logger.debug("State changed: {} -> {}", state, new_state);
				state = new_state;
				stateChanged.signalAll();
			} finally {
				stateLock.unlock();
			}
		}
		
		/**
		 * Change state if current state equals to expected.
		 * <p>
		 * @param expected - expected current state
		 * @param new_state - new state to set if condition is met
		 * @return true if new state was set, false otherwise
		 */
		public boolean changeStateIf(AggregatorState expected, AggregatorState new_state) {
			if ( state != expected ) {
				return false;
			}
			stateLock.lock();
			try {
				if ( state == expected ) {
					logger.debug("State changed: {} -> {}", state, new_state);
					state = new_state;
					stateChanged.signalAll();
					return true;
				} else {
					return false;
				}
			} finally {
				stateLock.unlock();
			}
		}
		
		/**
		 * Notify that anything has changed.
		 */
		public void notifyChanged() {
			stateLock.lock();
			try {
				stateChanged.signalAll();
			} finally {
				stateLock.unlock();
			}
		}

		/**
		 * Wait for any change notification.
		 */
		public void waitForChanges() {
			stateLock.lock();
			try {
				stateChanged.await(1, TimeUnit.SECONDS);
			} catch ( InterruptedException e ) {
				logger.error("Unexpected interruption: ", e);
			} finally {
				stateLock.unlock();
			}
		}
		
		/**
		 * Wait for state change.
		 * <p>
		 * @param expected - expected state
		 * @param timeout - timeout milliseconds
		 * @return true if state changed to expected, false in case of timeout
		 */
		public boolean waitForStateChange(AggregatorState expected, long timeout) {
			if ( state == expected ) {
				return true;
			}
			final long start = clock.millis();
			stateLock.lock();
			try {
				long elapsed = 0;
				while ( state != expected ) {
					if ( elapsed > timeout ) {
						return false;
					} else {
						try {
							stateChanged.await(timeout - elapsed, TimeUnit.MILLISECONDS);
						} catch ( InterruptedException e ) { }
					}
					elapsed = clock.millis() - start;
				}
			} finally {
				stateLock.unlock();
			}
			return true;
		}
		
		/**
		 * Wait for state change from current expected to another.
		 * <p>
		 * @param expected - state that expected to be current
		 * @param timeout - timeout millisecods
		 * @return true if state changed to another, false in case of timeout
		 */
		public boolean waitForStateChangeFrom(AggregatorState expected, long timeout) {
			if ( state != expected ) {
				return true;
			}
			final long start = clock.millis();
			stateLock.lock();
			try {
				long elapsed = 0;
				while ( state == expected ) {
					if ( elapsed > timeout ) {
						return false;
					} else {
						try {
							stateChanged.await(timeout - elapsed, TimeUnit.MILLISECONDS);
						} catch ( InterruptedException e ) { }
					}
					elapsed = clock.millis() - start;
				}
			} finally {
				stateLock.unlock();
			}
			return true;
		}
		
		/**
		 * Wait for state transition from one of expected states to one of another expected states.
		 * <p>
		 * There are several cases when this call can return false. The first one is timeout.
		 * Another one is when unexpected state was detected during condition check.
		 * Unexpected state mean that there was detected state which is not listed neither in initial and
		 * target state lists. And one more case when state is in both lists: in initial and target.
		 * In this case transition is impossible.
		 * <p>
		 * @param initial - collection of expected states to transition from
		 * @param target - collection of expected states to transition to
		 * @param timeout - timeout milliseconds
		 * @return true if transition has been made, false otherwise
		 */
		public boolean
			waitForStateChange(Collection<AggregatorState> initial, Collection<AggregatorState> target, long timeout)
		{
			// do not check outside the lock because it make testing harder 
			final long start = clock.millis();
			stateLock.lock();
			try {
				long elapsed = 0;
				while ( elapsed <= timeout ) {
					AggregatorState local_state = state;
					int is_from = initial.contains(local_state) ? 1 : 0;
					int is_to = target.contains(local_state) ? 1 : 0;
					if ( is_from + is_to != 1 ) {
						// It's combination where:
						// 1) 0 - both lists do not contain current state
						// 2) 2 - both lists contain current state
						return false;
					}
					if ( is_to == 1 ) {
						return true;
					}					
					try {
						stateChanged.await(timeout - elapsed, TimeUnit.MILLISECONDS);
					} catch ( InterruptedException e ) { }
					elapsed = clock.millis() - start;
				}
			} finally {
				stateLock.unlock();
			}
			return false;
		}
		
	}
	
	private final IRecoverableStreamsController controller;
	private final int maxErrors;
	private final StateTracker state;
	private final AtomicInteger totalErrors;
	private final AtomicBoolean availabilityChanged;
	
	RecoverableStreamsService(IRecoverableStreamsController builder,
			int maxErrors,
			StateTracker state,
			AtomicInteger totalErrors,
			AtomicBoolean availabilityChanged)
	{
		this.controller = builder;
		this.maxErrors = maxErrors;
		this.state = state;
		this.totalErrors = totalErrors;
		this.availabilityChanged = availabilityChanged;
	}
	
	public RecoverableStreamsService(IRecoverableStreamsController controller, int maxErrors) {
		this(controller, maxErrors, new StateTracker(), new AtomicInteger(), new AtomicBoolean());
	}

	private IRecoverableStreamsHandler createHandler() {
		IRecoverableStreamsHandler handler = null;
		try {
			state.changeState(AggregatorState.STARTING);
			handler = controller.build(this);
			handler.start();
			return handler;
		} catch ( Throwable t ) {
			logger.error("Failed to create handler: ", t);
			state.changeState(AggregatorState.ERROR);
			if ( handler != null ) {
				handler.close(); // do not close with closeHandler
			}
			return null;
		}
	}
	
	private IRecoverableStreamsHandler closeHandler(IRecoverableStreamsHandler handler) {
		if ( handler == null ) {
			return null;
		}
		try {
			controller.onClose(handler);
		} finally {
			try {
				handler.close();
			} catch ( Exception e ) {
				logger.error("Error closing handler: ", e);
			}
		}
		return null;
	}
	
	public IRecoverableStreamsController getController() {
		return controller;
	}
	
	public int getMaxErrors() {
		return maxErrors;
	}
	
	public int getTotalErrors() {
		return totalErrors.get();
	}
	
	StateTracker getStateTracker() {
		return state;
	}
	
	public AggregatorState getState() {
		return state.getState();
	}
	
	public Mode getMode() {
		return state.getMode();
	}
	
	@Override
	public void start() throws ServiceException {
		switch ( state.getMode() ) {
		case PENDING:
			state.changeModeIf(Mode.PENDING, Mode.RUNNING);
			break;
		case RUNNING:
			break;
		case CLOSE:
			throw new IllegalStateException("Service closed");
		}
	}

	@Override
	public boolean startAndWaitConfirm(long timeout) {
		start();
		if ( ! state.waitForStateChange(
				Arrays.asList(AggregatorState.CREATED, AggregatorState.PENDING),
				Arrays.asList(AggregatorState.RUNNING, AggregatorState.STARTING, AggregatorState.ERROR),
				timeout) )
		{
			return false;
		}
		return state.waitForStateChange(AggregatorState.RUNNING, timeout);
	}

	@Override
	public void stop() {
		switch ( state.getMode() ) {
		case PENDING:
			break;
		case RUNNING:
			state.changeModeIf(Mode.RUNNING, Mode.PENDING);
			break;
		case CLOSE:
			throw new IllegalStateException("Service closed");
		}
	}

	@Override
	public boolean stopAndWaitConfirm(long timeout) {
		stop();
		return state.waitForStateChange(AggregatorState.PENDING, timeout);
	}
	
	@Override
	public void close() {
		switch ( state.getMode() ) {
		case PENDING:
		case RUNNING:
			state.changeModeIfNot(Mode.CLOSE, Mode.CLOSE);
			break;
		case CLOSE:
			break;
		}
	}

	@Override
	public void onStarted() {
		state.notifyChanged();
	}

	@Override
	public void onRecoverableError() {
		totalErrors.incrementAndGet();
		state.notifyChanged();
	}

	@Override
	public void onUnrecoverableError() {
		totalErrors.incrementAndGet();
		state.notifyChanged();
	}

	@Override
	public void onClose(boolean error_on_close) {
		if ( error_on_close ) {
			totalErrors.incrementAndGet();
		}
	}
	
	@Override
	public void onUnavailable() {
		if ( availabilityChanged.compareAndSet(false, true) ) {
			state.notifyChanged();
		}
	}
	
	@Override
	public void onAvailable() {
		if ( availabilityChanged.compareAndSet(false, true) ) {
			state.notifyChanged();
		}
	}
	
	@Override
	public void run() {
		state.changeState(AggregatorState.PENDING);
		boolean exit = false;
		IRecoverableStreamsHandler handler = null;
		Mode my_mode = Mode.PENDING, to_mode;
		while ( exit == false ) {
			to_mode = state.getMode();
			switch ( my_mode ) {
			case PENDING:
				switch ( to_mode ) {
				case RUNNING:
					totalErrors.set(0);
					handler = createHandler();
					break;
				case CLOSE:
					exit = true;
					break;
				case PENDING:
					state.waitForChanges();
					break;
				}
				break;
			case RUNNING:
				switch ( to_mode ) {
				case PENDING:
					handler = closeHandler(handler);
					state.changeState(AggregatorState.PENDING);
					break;
				case CLOSE:
					exit = true;
					handler = closeHandler(handler);
					break;
				case RUNNING:
					if ( handler != null ) {
						if ( availabilityChanged.compareAndSet(true, false) ) {
							if ( handler.available() ) {
								controller.onAvailable(handler);
							} else {
								controller.onUnavailable(handler);
							}
						} else if ( handler.unrecoverableError() ) {
							handler = closeHandler(handler);
							state.changeState(AggregatorState.ERROR);
						} else if ( handler.recoverableError() ) {
							handler = closeHandler(handler);
							if ( totalErrors.get() >= maxErrors ) {
								state.changeState(AggregatorState.ERROR);
							} else {
								handler = createHandler();
							}
						} else if ( handler.started()
							&& state.changeStateIf(AggregatorState.STARTING, AggregatorState.RUNNING) )
						{
							controller.onRunning(handler);
						} else {
							state.waitForChanges();
						}
					} else {
						// It's OK.
						// If we're here then an unrecoverable error occurred.
						// Restarting service is mandatory.
						state.waitForChanges();
					}
					break;
				}
				break;
			case CLOSE:
				throw new IllegalStateException();
			}
			my_mode = to_mode;
		}
		state.changeState(AggregatorState.DEAD);
	}
	
	@Override
	public boolean waitForStateChangeFrom(AggregatorState from, long timeout) {
		return state.waitForStateChangeFrom(from, timeout);
	}

	@Override
	public boolean waitForStateChangeTo(AggregatorState to, long timeout) {
		return state.waitForStateChange(to, timeout);
	}

}
