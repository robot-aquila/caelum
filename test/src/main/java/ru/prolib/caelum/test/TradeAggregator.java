package ru.prolib.caelum.test;

import java.time.Duration;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueStore;

import ru.prolib.caelum.core.ILBCandle;
import ru.prolib.caelum.core.ILBTrade;
import ru.prolib.caelum.core.LBCandleInitializer;
import ru.prolib.caelum.core.LBCandleMutable;
import ru.prolib.caelum.core.LBCandleSerde;
import ru.prolib.caelum.core.LBTradeAggregator;
import ru.prolib.caelum.core.LBTradeSerde;

public class TradeAggregator {

	public static void main(String[] args) {
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "caelum-trade-aggregator");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.99.100:32768");
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, LBTradeSerde.class.getName());
		
		final StreamsBuilder builder = new StreamsBuilder();
		
		KStream<String, ILBTrade> str_trades = builder.stream("caelum-trades");
		KGroupedStream<String, ILBTrade> str_grouped_trades = str_trades.groupByKey();
		KTable<String, LBCandleMutable> table = str_grouped_trades
				.aggregate(new LBCandleInitializer(), new LBTradeAggregator());

	}

}
