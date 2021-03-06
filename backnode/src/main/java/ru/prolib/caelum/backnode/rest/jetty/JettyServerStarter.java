package ru.prolib.caelum.backnode.rest.jetty;

import org.eclipse.jetty.server.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.prolib.caelum.lib.IService;
import ru.prolib.caelum.lib.ServiceException;

public class JettyServerStarter implements IService {
	private static final Logger logger = LoggerFactory.getLogger(JettyServerStarter.class);
	private final Server server;
	
	public JettyServerStarter(Server server) {
		this.server = server;
	}
	
	public Server getServer() {
		return server;
	}

	@Override
	public void start() throws ServiceException {
		try {
			server.start();
		} catch ( Exception e ) {
			throw new ServiceException("Jetty server has failed to start", e);
		}
		logger.debug("Jetty server started");
	}

	@Override
	public void stop() throws ServiceException {
		try {
			server.stop();
		} catch ( Exception e ) {
			throw new ServiceException("Jetty server has failed to stop", e);
		}
		logger.debug("Jetty server stopped");
	}

}
