package ru.prolib.caelum.aggregator;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import ru.prolib.caelum.core.HostInfo;
import ru.prolib.caelum.core.ICloseableIterator;
import ru.prolib.caelum.core.ITuple;

public class AggregatedDataResponse {
	private final boolean askAnotherHost;
	private final HostInfo hostInfo;
	private final ICloseableIterator<ITuple> result;
	
	public AggregatedDataResponse(HostInfo hostInfo, ICloseableIterator<ITuple> result) {
		this.askAnotherHost = false;
		this.hostInfo = hostInfo;
		this.result = result;
	}
	
	public AggregatedDataResponse(HostInfo hostInfo) {
		this.askAnotherHost = true;
		this.hostInfo = hostInfo;
		this.result = null;
	}
	
	/**
	 * Check that request cannot be fulfilled because the data located on another host.
	 * <p>
	 * In case if requested data is on remote host, the host info is the host can provide the data.
	 * In case if requested data available locally, the host info points to this host.
	 * <p>
	 * @return true if data on another host, false if data available locally
	 */
	public boolean askAnotherHost() {
		return askAnotherHost;
	}
	
	public HostInfo getHostInfo() {
		return hostInfo;
	}
	
	public ICloseableIterator<ITuple> getResult() {
		return result;
	}
	
	@Override
	public boolean equals(Object other) {
		if ( other == this ) {
			return true;
		}
		if ( other == null || other.getClass() != AggregatedDataResponse.class ) {
			return false;
		}
		AggregatedDataResponse o = (AggregatedDataResponse) other;
		return new EqualsBuilder()
				.append(o.askAnotherHost, askAnotherHost)
				.append(o.hostInfo, hostInfo)
				.append(o.result, result)
				.build();
	}
	
	@Override
	public int hashCode() {
		return new HashCodeBuilder(77801, 1001)
				.append(askAnotherHost)
				.append(hostInfo)
				.append(result)
				.build();
	}
	
	@Override
	public String toString() {
		return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
				.append("askAnotherHost", askAnotherHost)
				.append("hostInfo", hostInfo)
				.append("result", result)
				.build();
	}
}
