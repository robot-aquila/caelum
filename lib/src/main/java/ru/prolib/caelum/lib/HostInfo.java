package ru.prolib.caelum.lib;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

public class HostInfo {
	private final String host;
	private final int port;
	
	public HostInfo(String host, int port) {
		this.host = host;
		this.port = port;
	}
	
	public String getHost() {
		return host;
	}
	
	public int getPort() {
		return port;
	}
	
	@Override
	public int hashCode() {
		return new HashCodeBuilder(1002875, 51)
				.append(host)
				.append(port)
				.build();
	}
	
	@Override
	public String toString() {
		return host + ":" + port;
	}
	
	@Override
	public boolean equals(Object other) {
		if ( other == this ) {
			return true;
		}
		if ( other == null || other.getClass() != HostInfo.class ) {
			return false;
		}
		HostInfo o = (HostInfo) other;
		return new EqualsBuilder()
				.append(o.host, host)
				.append(o.port, port)
				.build();
	}

}
