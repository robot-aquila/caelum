package ru.prolib.caelum.service.itemdb.kafka;

import java.io.IOException;
import java.time.Clock;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;

import ru.prolib.caelum.lib.kafka.KafkaItem;
import ru.prolib.caelum.service.GeneralConfig;
import ru.prolib.caelum.service.IBuildingContext;
import ru.prolib.caelum.service.itemdb.IItemDatabaseService;
import ru.prolib.caelum.service.itemdb.IItemDatabaseServiceBuilder;
import ru.prolib.caelum.service.itemdb.kafka.utils.KafkaProducerService;
import ru.prolib.caelum.service.itemdb.kafka.utils.KafkaUtils;

public class KafkaItemDatabaseServiceBuilder implements IItemDatabaseServiceBuilder {
	private final KafkaUtils utils;
	private final Clock clock;
	
	public KafkaItemDatabaseServiceBuilder(KafkaUtils utils, Clock clock) {
		this.utils = utils;
		this.clock = clock;
	}
	
	public KafkaItemDatabaseServiceBuilder() {
		this(KafkaUtils.getInstance(), Clock.systemUTC());
	}
	
	public KafkaUtils getUtils() {
		return utils;
	}
	
	public Clock getClock() {
		return clock;
	}
	
	protected KafkaProducer<String, KafkaItem> createProducer(GeneralConfig config) {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getKafkaBootstrapServers());
		props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, config.getItemServiceKafkaTransactionalId());
		return utils.createProducer(props);
	}
	
	protected KafkaItemDatabaseService createService(GeneralConfig config, KafkaProducer<String, KafkaItem> producer) {
		return new KafkaItemDatabaseService(config, producer, utils, clock);
	}

	@Override
	public IItemDatabaseService build(IBuildingContext context) throws IOException {
		KafkaProducer<String, KafkaItem> producer = createProducer(context.getConfig());
		context.registerService(new KafkaProducerService(producer));
		return createService(context.getConfig(), producer);
	}
	
	@Override
	public int hashCode() {
		return 50098172;
	}
	
	@Override
	public boolean equals(Object other) {
		if ( other == this ) {
			return true;
		}
		if ( other == null || other.getClass() != KafkaItemDatabaseServiceBuilder.class ) {
			return false;
		}
		return true;
	}

}
