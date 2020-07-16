package ru.prolib.caelum.itemdb.kafka;

import java.util.Arrays;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

import ru.prolib.caelum.itemdb.IItemIterator;
import ru.prolib.caelum.core.IItem;
import ru.prolib.caelum.itemdb.IItemDatabaseService;
import ru.prolib.caelum.itemdb.ItemDataRequest;
import ru.prolib.caelum.itemdb.ItemDataRequestContinue;
import ru.prolib.caelum.itemdb.ItemDatabaseConfig;

public class KafkaItemDatabaseService implements IItemDatabaseService {
	private final KafkaItemDatabaseConfig config;
	private final KafkaProducer<String, KafkaItem> producer;
	private final KafkaUtils utils;
	
	KafkaItemDatabaseService(KafkaItemDatabaseConfig config,
			KafkaProducer<String, KafkaItem> producer,
			KafkaUtils utils)
	{
		this.config = config;
		this.producer = producer;
		this.utils = utils;
	}
	
	public KafkaItemDatabaseService(KafkaItemDatabaseConfig config, KafkaProducer<String, KafkaItem> producer) {
		this(config, producer, KafkaUtils.getInstance());
	}
	
	private KafkaConsumer<String, KafkaItem> createConsumer() {
		return utils.createConsumer(config.getConsumerKafkaProperties());
	}
	
	private long getLimit(ItemDataRequest request) {
		return Math.min(request.getLimit(), config.getInt(ItemDatabaseConfig.LIST_ITEMS_LIMIT));
	}

	private long getLimit(ItemDataRequestContinue request) {
		return Math.min(request.getLimit(), config.getInt(ItemDatabaseConfig.LIST_ITEMS_LIMIT));
	}
	
	public KafkaItemDatabaseConfig getConfig() {
		return config;
	}
	
	public KafkaProducer<String, KafkaItem> getProducer() {
		return producer;
	}
	
	public KafkaUtils getUtils() {
		return utils;
	}

	@Override
	public IItemIterator fetch(ItemDataRequest request) {
		KafkaConsumer<String, KafkaItem> consumer = createConsumer();
		KafkaItemInfo item_info = utils.getItemInfo(consumer, config.getSourceTopic(), request.getSymbol());
		if ( item_info.hasData() ) {
			TopicPartition tp = item_info.toTopicPartition();
			consumer.assign(Arrays.asList(tp));
			consumer.seek(tp, utils.getOffset(consumer, tp, request.getFrom(), item_info.getStartOffset()));
			return utils.createIterator(consumer, item_info, getLimit(request), request.getTo());
		} else {
			return utils.createIteratorStub(consumer, item_info, getLimit(request), request.getTo());
		}
	}

	@Override
	public IItemIterator fetch(ItemDataRequestContinue request) {
		KafkaConsumer<String, KafkaItem> consumer = createConsumer();
		KafkaItemInfo item_info = utils.getItemInfo(consumer, config.getSourceTopic(), request.getSymbol());
		if ( item_info.hasData() ) {
			TopicPartition tp = item_info.toTopicPartition();
			consumer.assign(Arrays.asList(tp));
			consumer.seek(tp, request.getOffset());
			return utils.createIterator(consumer, item_info, getLimit(request), request.getTo());
		} else {
			return utils.createIteratorStub(consumer, item_info, getLimit(request), request.getTo());
		}
	}

	@Override
	public void registerItem(IItem item) {
		producer.send(new ProducerRecord<>(config.getSourceTopic(), null, item.getTime(), item.getSymbol(),
			new KafkaItem(item.getValue(), item.getDecimals(), item.getVolume(), item.getVolumeDecimals(),
					item.getType())));
	}

}
