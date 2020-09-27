package ru.prolib.caelum.lib;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServiceStarter implements Runnable {
	private static final Logger logger = LoggerFactory.getLogger(ServiceStarter.class);
	
	private final IService service;
	
	public ServiceStarter(IService service) {
		this.service = service;
	}

	@Override
	public void run() {
		try {
			service.start();
		} catch ( Throwable t ) {
			logger.error("Failed to start service: ", t);
		}
	}
	
	@Override
	public int hashCode() {
		return new HashCodeBuilder(113801, 59)
				.append(service)
				.build();
	}
	
	@Override
	public boolean equals(Object other) {
		if ( other == this ) {
			return true;
		}
		if ( other == null || other.getClass() != ServiceStarter.class ) {
			return false;
		}
		ServiceStarter o = (ServiceStarter) other;
		return new EqualsBuilder()
				.append(o.service, service)
				.build();
	}

}
