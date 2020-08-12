package ru.prolib.caelum.aggregator.kafka;

import java.time.Clock;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class KafkaStreamsAvailability {
	private volatile boolean available;
	private final Lock lock;
	private final Condition changed;
	private final Clock clock;
	
	KafkaStreamsAvailability(boolean available, Lock lock, Condition changed, Clock clock) {
		this.lock = lock;
		this.changed = changed;
		this.available = available;
		this.clock = clock;
	}
	
	public KafkaStreamsAvailability(boolean available, Lock lock, Clock clock) {
		this(available, lock, lock.newCondition(), clock);
	}
	
	public KafkaStreamsAvailability(boolean available) {
		this(available, new ReentrantLock(), Clock.systemUTC());
	}
	
	public KafkaStreamsAvailability() {
		this(true); // Initially streams are available after registration
	}
	
	public Lock getLock() {
		return lock;
	}
	
	public Condition getCondition() {
		return changed;
	}
	
	public Clock getClock() {
		return clock;
	}
	
	public boolean isAvailable() {
		return available;
	}
	
	public void setAvailable(boolean is_available) {
		boolean a = available;
		if ( a != is_available ) {
			lock.lock();
			try {
				if ( available != is_available ) {
					available = is_available;
					changed.signalAll();
				}
			} finally {
				lock.unlock();
			}
		}
	}
	
	/**
	 * Wait for availability change.
	 * <p>
	 * @param expected_availability - expected availability
	 * @param timeout - timeout milliseconds
	 * @return true if current availability equals to expected value, false in case of timeout
	 */
	public boolean waitForChange(boolean expected_availability, long timeout) {
		if ( available == expected_availability ) {
			return true;
		}
		if ( timeout <= 0 ) {
			throw new IllegalArgumentException();
		}
		final long start = clock.millis();
		lock.lock();
		try {
			long elapsed = 0;
			while ( available != expected_availability ) {
				if ( elapsed > timeout ) {
					return false;
				} else {
					try {
						changed.await(timeout - elapsed, TimeUnit.MILLISECONDS);
					} catch ( InterruptedException e ) { }
				}
				elapsed = clock.millis() - start;
			}
			
		} finally {
			lock.unlock();
		}
		return true;
	}

}
