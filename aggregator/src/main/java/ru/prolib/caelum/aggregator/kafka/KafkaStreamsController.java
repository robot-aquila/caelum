package ru.prolib.caelum.aggregator.kafka;

import java.util.concurrent.locks.Lock;

import ru.prolib.caelum.aggregator.kafka.utils.IRecoverableStreamsController;
import ru.prolib.caelum.aggregator.kafka.utils.IRecoverableStreamsHandler;
import ru.prolib.caelum.aggregator.kafka.utils.IRecoverableStreamsHandlerListener;
import ru.prolib.caelum.aggregator.kafka.utils.RecoverableStreamsHandler;
import ru.prolib.caelum.itemdb.kafka.utils.KafkaUtils;

public class KafkaStreamsController implements IRecoverableStreamsController {
	private final KafkaAggregatorDescr descr;
	private final KafkaAggregatorTopologyBuilder builder;
	private final KafkaAggregatorConfig config;
	private final KafkaStreamsRegistry registry;
	private final KafkaUtils utils;
	private final Lock cleanUpMutex;
	
	public KafkaStreamsController(KafkaAggregatorDescr descr,
			KafkaAggregatorTopologyBuilder builder,
			KafkaAggregatorConfig config,
			KafkaStreamsRegistry registry,
			Lock cleanUpMutex,
			KafkaUtils utils)
	{
		this.descr = descr;
		this.builder = builder;
		this.config = config;
		this.registry = registry;
		this.cleanUpMutex = cleanUpMutex;
		this.utils = utils;
	}
	
	public KafkaAggregatorDescr getDescriptor() {
		return descr;
	}
	
	public KafkaAggregatorTopologyBuilder getTopologyBuilder() {
		return builder;
	}
	
	public KafkaAggregatorConfig getConfig() {
		return config;
	}
	
	public KafkaStreamsRegistry getStreamsRegistry() {
		return registry;
	}
	
	public Lock getCleanUpMutex() {
		return cleanUpMutex;
	}
	
	public KafkaUtils getUtils() {
		return utils;
	}

	@Override
	public IRecoverableStreamsHandler build(IRecoverableStreamsHandlerListener listener) {
		return new RecoverableStreamsHandler(
				utils.createStreams(builder.buildTopology(config), config.getKafkaProperties()),
				listener,
				"aggregator-" + config.getAggregationPeriodCode().toLowerCase(),
				config.getDefaultTimeout(),
				cleanUpMutex);
	}
	
	@Override
	public void onRunning(IRecoverableStreamsHandler handler) {
		registry.register(descr, ((RecoverableStreamsHandler) handler).getStreams());
	}
	
	@Override
	public void onClose(IRecoverableStreamsHandler handler) {
		registry.deregister(descr);
	}
	
	@Override
	public void onAvailable(IRecoverableStreamsHandler handler) {
		registry.setAvailability(descr, handler.available());
	}
	
	@Override
	public void onUnavailable(IRecoverableStreamsHandler handler) {
		registry.setAvailability(descr, handler.available());
	}

}
