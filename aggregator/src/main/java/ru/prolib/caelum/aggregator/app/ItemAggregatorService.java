package ru.prolib.caelum.aggregator.app;

import org.apache.kafka.streams.KafkaStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ru.prolib.caelum.aggregator.ItemAggregatorConfig;
import ru.prolib.caelum.core.IService;

public class ItemAggregatorService implements IService {
	private static final Logger logger = LoggerFactory.getLogger(ItemAggregatorService.class);
	private final ItemAggregatorConfig conf;
	private final KafkaStreams streams;
	
	public ItemAggregatorService(ItemAggregatorConfig conf, KafkaStreams streams) {
		this.conf = conf;
		this.streams = streams;
	}
	
	private String getPeriodCode() {
		return conf.getString(ItemAggregatorConfig.AGGREGATION_PERIOD);
	}

	@Override
	public void start() {
		logger.debug("Starting up item aggregator by {}", getPeriodCode());
		conf.print(logger);

		streams.cleanUp();
		streams.start();
	}

	@Override
	public void stop() {
		streams.close();
		logger.info("Finished item aggregator by {}", getPeriodCode());
	}

}
