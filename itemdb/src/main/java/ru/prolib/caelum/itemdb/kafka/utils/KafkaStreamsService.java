package ru.prolib.caelum.itemdb.kafka.utils;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.kafka.streams.KafkaStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.prolib.caelum.core.AbstractConfig;
import ru.prolib.caelum.core.IService;

public class KafkaStreamsService implements IService {
	static final Logger logger = LoggerFactory.getLogger(KafkaStreamsService.class);
	private final KafkaStreams streams;
	private final String serviceName;
	private final AbstractConfig config;
	
	public KafkaStreamsService(KafkaStreams streams, String serviceName, AbstractConfig config) {
		this.streams = streams;
		this.serviceName = serviceName;
		this.config = config;
	}
	
	public KafkaStreamsService(KafkaStreams streams, String serviceName) {
		this(streams, serviceName, null);
	}
	
	@Override
	public void start() {
		logger.debug("Starting up " + serviceName);
		if ( config != null ) {
			config.print(logger);
		}
		streams.cleanUp();
		streams.start();
	}

	@Override
	public void stop() {
		streams.close();
		logger.info("Finished " + serviceName);
	}
	
	@Override
	public int hashCode() {
		return new HashCodeBuilder(18989261, 15)
				.append(streams)
				.append(serviceName)
				.append(config)
				.build();
	}
	
	@Override
	public String toString() {
		return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
				.append("serviceName", serviceName)
				.append("streams", streams)
				.append("config", config)
				.build();
	}
	
	@Override
	public boolean equals(Object other) {
		if ( other == this ) {
			return true;
		}
		if ( other == null || other.getClass() != KafkaStreamsService.class ) {
			return false;
		}
		KafkaStreamsService o = (KafkaStreamsService) other;
		return new EqualsBuilder()
				.append(o.streams, streams)
				.append(o.serviceName, serviceName)
				.append(o.config, config)
				.build();
	}

}
