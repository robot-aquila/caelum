package ru.prolib.caelum.aggregator.kafka.utils;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.State;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.prolib.caelum.core.ConditionalBitwiseOperator;
import ru.prolib.caelum.core.BitsSetIfUnset;

/**
 * Thread-safe recoverable streams handler.
 * It does state adaptation and retranslation it to listener as well as safe closing of streams.
 * <p>
 * <b>Warning:</b> Don't call any streams methods during callbacks, use executors to schedule calls instead.
 */
public class RecoverableStreamsHandler implements IRecoverableStreamsHandler, KafkaStreams.StateListener {
	private static final Logger logger = LoggerFactory.getLogger(RecoverableStreamsHandler.class);
	private static final int STARTED 				= 0x01;
	private static final int ERROR					= 0x02;
	private static final int CLOSED					= 0x04;
	private static final int ERROR_MASK				= STARTED | ERROR;
	private static final int RECOVERABLE_ERROR		= ERROR_MASK;
	private static final int UNRECOVERABLE_ERROR	= ERROR;
	
	private final KafkaStreams streams;
	private final AtomicReference<IRecoverableStreamsHandlerListener> listener;
	private final String serviceName;
	private final long shutdownTimeout;
	private final AtomicInteger state;
	
	RecoverableStreamsHandler(KafkaStreams streams,
			AtomicReference<IRecoverableStreamsHandlerListener> listener,
			String serviceName,
			long shutdownTimeout,
			AtomicInteger state)
	{
		this.streams = streams;
		this.listener = listener;
		this.serviceName = serviceName;
		this.shutdownTimeout = shutdownTimeout;
		this.state = state;
	}
	
	public RecoverableStreamsHandler(KafkaStreams streams,
			IRecoverableStreamsHandlerListener listener,
			String serviceName,
			long shutdownTimeout)
	{
		this(streams, new AtomicReference<>(listener), serviceName, shutdownTimeout, new AtomicInteger());
	}
	
	public KafkaStreams getStreams() {
		return streams;
	}
	
	public IRecoverableStreamsHandlerListener getStateListener() {
		return listener.get();
	}
	
	public String getServiceName() {
		return serviceName;
	}
	
	public long getShutdownTimeout() {
		return shutdownTimeout;
	}
	
	@Override
	public boolean closed() {
		return (state.get() & CLOSED) == CLOSED;
	}
	
	@Override
	public boolean recoverableError() {
		return (state.get() & ERROR_MASK) == RECOVERABLE_ERROR;
	}
	
	@Override
	public boolean unrecoverableError() {
		return (state.get() & ERROR_MASK) == UNRECOVERABLE_ERROR;
	}
	
	public boolean started() {
		return (state.get() & (CLOSED | STARTED)) == STARTED;
	}
	
	@Override
	public void onChange(State newState, State oldState) {
		final ConditionalBitwiseOperator op;
		switch ( newState ) {
		case RUNNING:
			state.accumulateAndGet(STARTED, op = new BitsSetIfUnset(CLOSED | ERROR | STARTED));
			if ( op.applied() ) listener.get().onStarted();
			break;
		case ERROR:
			int s = state.accumulateAndGet(ERROR, op = new BitsSetIfUnset(CLOSED | ERROR));
			if ( op.applied() ) {
				if ( (s & STARTED) == STARTED ) {
					logger.warn("Service error (recoverable): {}", serviceName);
					listener.get().onRecoverableError();
				} else {
					logger.error("Service error (unrecoverable): {}", serviceName);
					listener.get().onUnrecoverableError();
				}
			}
			break;
		default:
			break;
		}
	}

	@Override
	public void close() {
		final ConditionalBitwiseOperator op = new BitsSetIfUnset(CLOSED);
		state.accumulateAndGet(CLOSED, op);
		if ( op.applied() ) {
			boolean error_on_close = streams.close(Duration.ofMillis(shutdownTimeout)) == false;
			if ( error_on_close ) {
				logger.warn("Timeout while shutdown service: {}", serviceName);
			} else {
				logger.debug("Service stopped: {}", serviceName);
			}
			listener.getAndSet(new RecoverableStreamsStateListenerStub()).onClose(error_on_close);
		}
	}
	
	@Override
	public void start() {
		streams.setStateListener(this);
		streams.cleanUp();
		streams.start();
	}

}
