package ru.prolib.caelum.core;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CompositeService implements IService {
	private static final Logger logger = LoggerFactory.getLogger(CompositeService.class);
	private final List<IService> registered;
	private final LinkedList<IService> started;
	
	CompositeService(List<IService> registered, LinkedList<IService> started) {
		this.registered = registered;
		this.started = started;
	}
	
	public CompositeService() {
		this(new ArrayList<>(), new LinkedList<>());
	}
	
	public synchronized CompositeService register(IService service) throws ServiceException {
		if ( started.size() > 0 ) {
			throw new ServiceException("Cannot register when started");
		}
		registered.add(service);
		return this;
	}

	@Override
	public synchronized void start() throws ServiceException {
		if ( started.size() > 0 ) {
			throw new ServiceException("Service already started");
		}
		for ( IService service : registered ) {
			try {
				service.start();
				started.add(service);
			} catch ( Throwable t ) {
				try {
					stop();
				} catch ( ServiceException e ) { }
				throw new ServiceException("Error starting service", t);
			}
		}
	}

	@Override
	public synchronized void stop() throws ServiceException {
		Throwable last_error = null;
		while ( started.size() > 0 ) {
			try {
				started.removeLast().stop();
			} catch ( Throwable t ) {
				last_error = t;
				logger.error("Unexpected exception: ", t);
			}
		}
		if ( last_error != null ) {
			throw new ServiceException("At least one error while stopping service", last_error);
		}
	}

}
