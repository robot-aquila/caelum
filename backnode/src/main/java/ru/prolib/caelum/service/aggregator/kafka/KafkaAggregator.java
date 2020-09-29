package ru.prolib.caelum.service.aggregator.kafka;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.streams.KafkaStreams;

import ru.prolib.caelum.service.AggregatorStatus;
import ru.prolib.caelum.service.aggregator.IAggregator;
import ru.prolib.caelum.service.aggregator.kafka.utils.IRecoverableStreamsService;
import ru.prolib.caelum.service.itemdb.kafka.utils.KafkaUtils;

public class KafkaAggregator implements IAggregator {
	private final KafkaAggregatorDescr descr;
	private final KafkaAggregatorConfig config;
	private final IRecoverableStreamsService streamsService;
	private final KafkaUtils utils;
	private final KafkaStreamsRegistry registry;
	
	public KafkaAggregator(KafkaAggregatorDescr descr,
			KafkaAggregatorConfig config,
			IRecoverableStreamsService streamsService,
			KafkaUtils utils,
			KafkaStreamsRegistry registry)
	{
		this.descr = descr;
		this.config = config;
		this.streamsService = streamsService;
		this.utils = utils;
		this.registry = registry;
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
	
	public KafkaStreamsRegistry getStreamsRegistry() {
		return registry;
	}
	
	@Override
	public AggregatorStatus getStatus() {
		KafkaAggregatorEntry entry = registry.getByInterval(descr.getInterval());
		return new AggregatorStatus(
				"AK",
				descr.getInterval(),
				descr.getType(),
				streamsService.getState(),
				new KafkaAggregatorStatusInfo(descr.getSource(),
						descr.getTarget(),
						descr.getStoreName(),
						entry != null ? entry.isAvailable() : false,
						entry != null ? entry.getStreamsState() : KafkaStreams.State.NOT_RUNNING
					)
			);
	}
	
	private String getChangelogTopic() {
		return config.getApplicationId() + "-" + config.getStoreName() + "-changelog";
	}
	
	@Override
	public void clear(boolean global) {
		final long timeout = config.getDefaultTimeout();
		if ( ! streamsService.stopAndWaitConfirm(timeout) ) {
			throw new IllegalStateException("Failed to stop streams service: " + descr.getInterval());
		}
		if ( global ) {
			try ( AdminClient admin = utils.createAdmin(config.getAdminClientProperties()) ) {
				utils.deleteRecords(admin, getChangelogTopic(), timeout);
				if ( config.getTargetTopic() != null ) {
					utils.deleteRecords(admin,  config.getTargetTopic(), timeout);
				}
			}
		}
		streamsService.startAndWaitConfirm(timeout);
	}

}
