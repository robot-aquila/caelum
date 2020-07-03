package ru.prolib.caelum.itemdb.kafka;

import java.util.Arrays;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import ru.prolib.caelum.itemdb.IItemIterator;
import ru.prolib.caelum.itemdb.IItemDatabaseService;
import ru.prolib.caelum.itemdb.ItemDataRequest;
import ru.prolib.caelum.itemdb.ItemDataRequestContinue;
import ru.prolib.caelum.itemdb.ItemDatabaseConfig;

public class ItemDatabaseService implements IItemDatabaseService {
	private final ItemDatabaseConfig config;
	private final KafkaUtils utils;
	
	ItemDatabaseService(ItemDatabaseConfig config, KafkaUtils utils) {
		this.config = config;
		this.utils = utils;
	}
	
	public ItemDatabaseService(ItemDatabaseConfig config) {
		this(config, KafkaUtils.getInstance());
	}
	
	private KafkaConsumer<String, KafkaItem> createConsumer() {
		return utils.createConsumer(config.getKafkaProperties());
	}
	
	private long getLimit(ItemDataRequest request) {
		return Math.min(request.getLimit(), config.getInt(ItemDatabaseConfig.LIMIT));
	}

	private long getLimit(ItemDataRequestContinue request) {
		return Math.min(request.getLimit(), config.getInt(ItemDatabaseConfig.LIMIT));
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

}
