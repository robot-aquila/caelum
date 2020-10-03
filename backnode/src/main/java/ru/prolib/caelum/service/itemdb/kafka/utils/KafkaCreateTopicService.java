package ru.prolib.caelum.service.itemdb.kafka.utils;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.prolib.caelum.lib.IService;
import ru.prolib.caelum.lib.ServiceException;
import ru.prolib.caelum.service.GeneralConfig;

public class KafkaCreateTopicService implements IService {
	private static final Logger logger = LoggerFactory.getLogger(KafkaCreateTopicService.class);
	private final KafkaUtils utils;
	private final GeneralConfig config;
	private final NewTopic topicDescr;
	private final long timeout;
	
	public KafkaCreateTopicService(KafkaUtils utils, GeneralConfig config, NewTopic topicDescr, long timeout) {
		this.utils = utils;
		this.config = config;
		this.topicDescr = topicDescr;
		this.timeout = timeout;
	}
	
	public KafkaUtils getUtils() {
		return utils;
	}
	
	public GeneralConfig getConfig() {
		return config;
	}
	
	public NewTopic getTopicDescriptor() {
		return topicDescr;
	}
	
	public long getTimeout() {
		return timeout;
	}
	
	@Override
	public void start() throws ServiceException {
		try ( AdminClient admin = utils.createAdmin(config) ) {
			utils.createTopic(admin, topicDescr, timeout);
		} catch ( InterruptedException|TimeoutException e ) {
			throw new ServiceException("Unexpected exception: ", e);
		} catch ( ExecutionException e ) {
			logger.error("Failed to create topic (possible concurrent creation): ", e);
		}
	}

	@Override
	public void stop() throws ServiceException {
		
	}
	
	@Override
	public int hashCode() {
		return new HashCodeBuilder(115237, 29)
				.append(utils)
				.append(config)
				.append(topicDescr)
				.append(timeout)
				.build();
	}
	
	@Override
	public boolean equals(Object other) {
		if ( other == this ) {
			return true;
		}
		if ( other == null || other.getClass() != KafkaCreateTopicService.class ) {
			return false;
		}
		KafkaCreateTopicService o = (KafkaCreateTopicService) other;
		return new EqualsBuilder()
				.append(o.utils, utils)
				.append(o.config, config)
				.append(o.topicDescr, topicDescr)
				.append(o.timeout, timeout)
				.build();
	}
	
	@Override
	public String toString() {
		return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
				.append("utils", utils)
				.append("config", config)
				.append("topicDescr", topicDescr)
				.append("timeout", timeout)
				.build();
	}

}
