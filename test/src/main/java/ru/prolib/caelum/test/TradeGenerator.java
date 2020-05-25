package ru.prolib.caelum.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.BasicConfigurator;

import ru.prolib.caelum.core.ILBTrade;
import ru.prolib.caelum.core.LBTrade;

public class TradeGenerator {
	
	static class SymbolDesc {
		final String symbol;
		final byte priceDecimals, volumeDecimals;
		long lastPrice, totalCount;
		
		public SymbolDesc(String symbol, byte price_decimals, byte volume_decimals, long last_price) {
			this.symbol = symbol;
			this.priceDecimals = price_decimals;
			this.volumeDecimals = volume_decimals;
			this.lastPrice = last_price;
		}
		
		@Override
		public String toString() {
			return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
		}

	}

	public static void main(String[] args) throws Exception {
		new TradeGenerator().run(args);
	}
	
	private Random rand;
	
	String randomString(int length) {
		final String alpha = "ABCDEFGHIJKLMNOPQRSTUVWXYZ123456789";
		StringBuilder sb = new StringBuilder();
		while ( length-- != 0 ) {
			sb.append(alpha.charAt(rand.nextInt(alpha.length())));
		}
		return sb.toString();
	}
	
	void print(Object msg) {
		System.out.println(msg);
	}
	
	Map<String, SymbolDesc> randomSymbols(int num, int chars, String prefix, String suffix) {
		Map<String, SymbolDesc> result = new HashMap<>();
		for ( int i = 0; i < num * 2; i ++ ) {
			String symbol = prefix + randomString(chars) + suffix;
			if ( ! result.containsKey(symbol) ) {
				result.put(symbol, new SymbolDesc(symbol,
						(byte)rand.nextInt(16), (byte)rand.nextInt(16), rand.nextLong()));
			}
			if ( result.size() >= num ) {
				break;
			}
		}
		return result;
	}
	
	ILBTrade randomTrade(SymbolDesc desc) {
		desc.lastPrice = rand.nextLong(); 
		desc.totalCount ++;
		return new LBTrade(desc.lastPrice, desc.priceDecimals, rand.nextLong(), desc.volumeDecimals);
	}
	
	public void run(String[] args) throws Exception {
		BasicConfigurator.configure();
		TradeGeneratorConfig conf = new TradeGeneratorConfig();
		conf.loadFromResources(TradeGeneratorConfig.DEFAULT_CONFIG_FILE);
		if ( args.length > 0 ) {
			if ( ! conf.loadFromFile(args[0]) ) {
				throw new IOException("Error loading config: " + args[0]);
			} else {
				conf.loadFromFile(TradeGeneratorConfig.DEFAULT_CONFIG_FILE);
			}
		}
		print("- Configuration --------------------------");
		conf.print(System.out);
		
		rand = new Random(conf.getInt(TradeGeneratorConfig.SEED));
		Map<String, SymbolDesc> symbols = randomSymbols(conf.getInt(TradeGeneratorConfig.SYMBOL_NUM, 1, 255),
				conf.getInt(TradeGeneratorConfig.SYMBOL_CHARS, 3, 10),
				conf.getString(TradeGeneratorConfig.SYMBOL_PREFIX),
				conf.getString(TradeGeneratorConfig.SYMBOL_SUFFIX));
		List<String> symbol_list = new ArrayList<>(symbols.keySet());
		Collections.sort(symbol_list);
		print("- Symbol List ----------------------------");
		for ( String symbol : symbol_list ) {
			print(symbols.get(symbol));
		}

		int freq = 60000 / conf.getInt(TradeGeneratorConfig.TRADES_PER_MINUTE, 1, 60000);
		int freq_half = freq / 2;
		AtomicLong start_time = new AtomicLong(System.currentTimeMillis()), total_trades = new AtomicLong(0L);
		final KafkaProducer<String, ILBTrade> producer = new KafkaProducer<>(conf.getKafkaProperties());
		final String topic = conf.getString(TradeGeneratorConfig.TARGET_TOPIC);
		final CountDownLatch finished = new CountDownLatch(1);
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override public void run() {
				print("  Total time: " + (System.currentTimeMillis() - start_time.get()) + "ms");
				print("Total trades: " + total_trades.get());
				producer.close();
				finished.countDown();
			}
		});

		try {
			while ( finished.getCount() > 0 ) {
				long wait = freq_half + rand.nextInt(freq);
				print("   Wait for: " + wait + "ms");
				Thread.sleep(wait);
				SymbolDesc symbol = symbols.get(symbol_list.get(rand.nextInt(symbol_list.size())));
				ILBTrade trade = randomTrade(symbol);
				long time = System.currentTimeMillis();
				total_trades.incrementAndGet();
				print("Symbol Desc: " + symbol);
				print("      Trade: " + trade);
				print("       Time: " + time);
				producer.send(new ProducerRecord<>(topic, null, time, symbol.symbol, trade));
			}
		} catch ( InterruptedException e ) {
			print("Interrupted...");
		}
	}

}
