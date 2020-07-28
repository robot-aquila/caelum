package ru.prolib.caelum.aggregator.kafka.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.prolib.caelum.aggregator.AggregatorState;
import ru.prolib.caelum.core.IService;
import ru.prolib.caelum.core.ServiceException;

public class RecoverableStreamsServiceStarter implements IService {
	private static final Logger logger = LoggerFactory.getLogger(RecoverableStreamsServiceStarter.class);
	
	private final Thread thread;
	private final IRecoverableStreamsService service;
	private final long timeout;
	
	public RecoverableStreamsServiceStarter(Thread thread, IRecoverableStreamsService service, long timeout) {
		this.thread = thread;
		this.service = service;
		this.timeout = timeout;
	}
	
	public Thread getThread() {
		return thread;
	}
	
	public IRecoverableStreamsService getStreamsService() {
		return service;
	}
	
	public long getTimeout( ) {
		return timeout;
	}

	@Override
	public void start() throws ServiceException {
		thread.start();
		if ( service.waitForStateChangeFrom(AggregatorState.CREATED, timeout) == false ) {
			throw new ServiceException("Timeout while starting service");
		} else {
			logger.debug("Streams service thread has been started: {}", thread.getName());
		}
		service.start();
	}

	@Override
	public void stop() throws ServiceException {
		service.close();
		if ( service.waitForStateChangeTo(AggregatorState.DEAD, timeout) == false ) {
			logger.error("Timeout while shutting down service");
		}
		if ( thread.isAlive() ) {
			logger.warn("Streams service thread was not terminated: {}", thread.getName());
		} else {
			logger.debug("Streams service thread has been terminated: {}", thread.getName());
		}
	}

}
