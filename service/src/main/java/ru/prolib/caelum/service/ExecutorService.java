package ru.prolib.caelum.service;

import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.prolib.caelum.core.IService;
import ru.prolib.caelum.core.ServiceException;

public class ExecutorService implements IService {
	private static final Logger logger = LoggerFactory.getLogger(ExecutorService.class);
	private final java.util.concurrent.ExecutorService executor;
	private final long shutdownTimeout;
	
	/**
	 * Constructor.
	 * <p>
	 * @param executor - executor service instance
	 * @param shutdown_timeout - executor shutdown timeout in milliseconds
	 */
	public ExecutorService(java.util.concurrent.ExecutorService executor, long shutdown_timeout) {
		this.executor = executor;
		this.shutdownTimeout = shutdown_timeout;
	}
	
	public java.util.concurrent.ExecutorService getExecutor() {
		return executor;
	}
	
	public long getShutdownTimeout() {
		return shutdownTimeout;
	}

	@Override
	public void start() throws ServiceException {
		
	}

	@Override
	public void stop() throws ServiceException {
		executor.shutdown();
		try {
			if ( executor.awaitTermination(shutdownTimeout, TimeUnit.MILLISECONDS) == false ) {
				logger.warn("Timeout while shutdown executor: {} ms.", shutdownTimeout);
				executor.shutdownNow();
				if ( executor.awaitTermination(shutdownTimeout, TimeUnit.MILLISECONDS) == false ) {
					logger.warn("Executor did not terminate in {} ms.", shutdownTimeout * 2);
				}
			}
		} catch ( InterruptedException e ) {
			logger.warn("Thread interrupted: ", e);
			executor.shutdownNow();
			Thread.currentThread().interrupt();
		}
	}
	
	@Override
	public int hashCode() {
		return new HashCodeBuilder(1782191049, 95)
				.append(executor)
				.append(shutdownTimeout)
				.build();
	}
	
	@Override
	public boolean equals(Object other) {
		if ( other == this ) {
			return true;
		}
		if ( other == null || other.getClass() != ExecutorService.class ) {
			return false;
		}
		ExecutorService o = (ExecutorService) other;
		return new EqualsBuilder()
				.append(o.executor, executor)
				.append(o.shutdownTimeout, shutdownTimeout)
				.build();
	}

}
