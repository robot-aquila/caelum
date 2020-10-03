package ru.prolib.caelum.service.itesym;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.prolib.caelum.lib.IService;
import ru.prolib.caelum.lib.ServiceException;
import ru.prolib.caelum.service.ExtensionState;
import ru.prolib.caelum.service.ExtensionStatus;
import ru.prolib.caelum.service.ICaelum;
import ru.prolib.caelum.service.IExtension;

public class Itesym implements IExtension, IService, Runnable {
	private static final Logger logger = LoggerFactory.getLogger(Itesym.class);
	private final String groupId, sourceTopic;
	private final KafkaConsumer<String, byte[]> consumer;
	private final ICaelum caelum;
	private final long pollTimeout, shutdownTimeout;
	private final AtomicReference<Thread> thread;
	private final AtomicBoolean close, clear, started, stopped;
	private volatile ExtensionState state = ExtensionState.CREATED;
	
	Itesym(String groupId,
			KafkaConsumer<String, byte[]> consumer,
			String sourceTopic,
			ICaelum caelum,
			long pollTimeout,
			long shutdownTimeout,
			AtomicReference<Thread> thread,
			AtomicBoolean close,
			AtomicBoolean clear,
			AtomicBoolean started,
			AtomicBoolean stopped)
	{
		this.groupId = groupId;
		this.consumer = consumer;
		this.sourceTopic = sourceTopic;
		this.caelum = caelum;
		this.pollTimeout = pollTimeout;
		this.shutdownTimeout = shutdownTimeout;
		this.thread = thread;
		this.close = close;
		this.clear = clear;
		this.started = started;
		this.stopped = stopped;
	}
	
	public Itesym(String groupId,
			KafkaConsumer<String, byte[]> consumer,
			String sourceTopic,
			ICaelum caelum,
			long pollTimeout,
			long shutdownTimeout,
			AtomicReference<Thread> thread)
	{
		this(groupId, consumer, sourceTopic, caelum, pollTimeout, shutdownTimeout, thread,
				new AtomicBoolean(), new AtomicBoolean(), new AtomicBoolean(), new AtomicBoolean());
	}
	
	public String getGroupId() {
		return groupId;
	}
	
	public KafkaConsumer<String, byte[]> getConsumer() {
		return consumer;
	}
	
	public String getSourceTopic() {
		return sourceTopic;
	}
	
	public ICaelum getCaelum() {
		return caelum;
	}
	
	public long getPollTimeout() {
		return pollTimeout;
	}
	
	public long getShutdownTimeout() {
		return shutdownTimeout;
	}
	
	public Thread getThread() {
		return thread.get();
	}
	
	@Override
	public ExtensionStatus getStatus() {
		return new ExtensionStatus(state, null);
	}

	@Override
	public void clear() {
		if ( clear.compareAndSet(false, true) ) {
			consumer.wakeup();
		}
	}

	@Override
	public void start() throws ServiceException {
		if ( started.compareAndSet(false, true) ) {
			thread.get().start();
		}
	}

	@Override
	public void stop() throws ServiceException {
		if ( stopped.compareAndSet(false, true) ) {
			close.compareAndSet(false, true);
			consumer.wakeup();
			try {
				thread.get().join(shutdownTimeout);
			} catch ( InterruptedException e ) {
				logger.warn("[{}] Unexpected interruption: ", groupId, e);
			}
			if ( thread.get().isAlive() ) {
				logger.warn("[{}] Worker thread did not ternimated", groupId);
			}
		}
	}
	
	@Override
	public void run() {
		final Map<String, Object> cache = new HashMap<>();
		final Set<String> batch = new HashSet<>();
		final Object marker = new Object();
		state = ExtensionState.RUNNING;
		final Duration d = Duration.ofMillis(pollTimeout);
		consumer.subscribe(Arrays.asList(sourceTopic));
		try {
			while ( close.get() == false ) {
				try {
					ConsumerRecords<String, byte[]> records = consumer.poll(d);
					for ( ConsumerRecord<String, byte[]> r : records ) {
						final String symbol = r.key();
						if ( cache.putIfAbsent(symbol, marker) == null ) {
							batch.add(symbol);
						}
					}
					if ( batch.size() > 0 ) {
						try {
							caelum.registerSymbol(batch);
						} catch ( Exception e ) {
							logger.error("[{}] Unexpected exception: ", groupId, e);
						} finally {
							batch.clear();
						}
					}
					consumer.commitSync();
				} catch ( WakeupException e ) {
					if ( clear.compareAndSet(true, false) ) {
						cache.clear();
					}
				}
			}
		} catch ( Throwable t ) {
			logger.error("[{}] Unexpected exception: ", groupId, t);
		} finally {
			clear.set(true);
			close.set(true);
			state = ExtensionState.DEAD;
			consumer.close();
		}
	}

}
