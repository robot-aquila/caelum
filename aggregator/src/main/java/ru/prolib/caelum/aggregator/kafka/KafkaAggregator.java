package ru.prolib.caelum.aggregator.kafka;

import org.apache.kafka.clients.admin.AdminClient;

import ru.prolib.caelum.aggregator.AggregatorStatus;
import ru.prolib.caelum.aggregator.IAggregator;
import ru.prolib.caelum.aggregator.kafka.utils.IRecoverableStreamsService;
import ru.prolib.caelum.itemdb.kafka.utils.KafkaUtils;

public class KafkaAggregator implements IAggregator {
	private final KafkaAggregatorDescr descr;
	private final KafkaAggregatorConfig config;
	private final IRecoverableStreamsService streamsService;
	private final KafkaUtils utils;
	
	public KafkaAggregator(KafkaAggregatorDescr descr,
			KafkaAggregatorConfig config,
			IRecoverableStreamsService streamsService,
			KafkaUtils utils)
	{
		this.descr = descr;
		this.config = config;
		this.streamsService = streamsService;
		this.utils = utils;
	}
	
	public KafkaAggregatorDescr getDescriptor() {
		return descr;
	}
	
	public KafkaAggregatorConfig getConfig() {
		return config;
	}
	
	public IRecoverableStreamsService getStreamsService() {
		return streamsService;
	}
	
	public KafkaUtils getUtils() {
		return utils;
	}

	@Override
	public AggregatorStatus getStatus() {
		return new AggregatorStatus(
				descr.getPeriod(),
				descr.getType(),
				streamsService.getState(),
				new StringBuilder()
					.append("source=").append(descr.getSource())
					.append(" target=").append(descr.getTarget())
					.append(" store=").append(descr.getStoreName())
					.toString()
			);
	}
	
	private String getChangelogTopic() {
		return config.getApplicationId() + "-" + config.getStoreName() + "-changelog";
	}

	@Override
	public void clear() {
		final long timeout = config.getDefaultTimeout();
		if ( ! streamsService.stopAndWaitConfirm(timeout) ) {
			throw new IllegalStateException("Failed to stop streams service: " + descr.getPeriod());
		}
		try ( AdminClient admin = utils.createAdmin(config.getAdminClientProperties()) ) {
			utils.deleteRecords(admin, getChangelogTopic(), timeout);
			if ( config.getTargetTopic() != null ) {
				utils.deleteRecords(admin,  config.getTargetTopic(), timeout);
			}
		}		
		streamsService.startAndWaitConfirm(timeout);
	}

}
