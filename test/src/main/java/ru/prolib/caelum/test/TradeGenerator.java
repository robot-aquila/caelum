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

import ru.prolib.caelum.itemdb.kafka.KafkaItem;

public class TradeGenerator {
	static final double MAX_VOLATILITY = 0.30d, MIN_VOLATILITY = 0.01d, AVG_VOLATILITY = 0.15d;
	static final long VOLATILITY_RECALC_TRIGGER = 20L;
	static final long MIN_PRICE = 100L, MAX_PRICE = Long.MAX_VALUE;
	
	public static void main(String[] args) throws Exception {
		new TradeGenerator().run(args);
	}
	
	static void print(Object msg) {
		System.out.println(msg);
	}
	
	public static class SymbolDesc {
		final String symbol;
		final byte priceDecimals, volumeDecimals;
		long lastPrice, totalCount;
		double volatility = AVG_VOLATILITY;
		
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
		
		public double getVolatility() {
			return volatility;
		}
		
		public double recalcVolatility(double gaussian) {
			double old_volat = volatility;
			volatility = Math.abs(Math.min(Math.abs(gaussian / 3.0d), 1.0d))
					* (MAX_VOLATILITY - MIN_VOLATILITY) + MIN_VOLATILITY;
			return old_volat;
		}

	}
	
	public static class RandomUtils {
		private static final String CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ123456789";
		final Random rand;
		
		public RandomUtils(Random rand) {
			this.rand = rand;
		}
		
		public String randomString(int length) {
			StringBuilder sb = new StringBuilder();
			while ( length-- != 0 ) {
				sb.append(CHARS.charAt(rand.nextInt(CHARS.length())));
			}
			return sb.toString();
		}
		
		public SymbolDesc randomSymbol(int chars, String prefix, String suffix) {
			String symbol = prefix + randomString(chars) + suffix;
			SymbolDesc desc = new SymbolDesc(symbol, (byte)rand.nextInt(16),
					(byte)rand.nextInt(16), rand.nextLong());
			desc.lastPrice = (long) Math.pow(10, 2 + rand.nextInt(16));
			return desc;
		}

		public Map<String, SymbolDesc> randomSymbols(int num, int chars, String prefix, String suffix) {
			Map<String, SymbolDesc> result = new HashMap<>();
			for ( int i = 0; i < num * 2; i ++ ) {
				SymbolDesc desc = randomSymbol(chars, prefix, suffix);
				if ( ! result.containsKey(desc.symbol) ) {
					result.put(desc.symbol, desc);
				}
				if ( result.size() >= num ) {
					break;
				}
			}
			return result;
		}

		KafkaItem randomTrade(SymbolDesc desc) {
			desc.totalCount ++;
			if ( desc.totalCount % VOLATILITY_RECALC_TRIGGER == 0 ) {
				double old_volatility = desc.recalcVolatility(rand.nextGaussian());
				print(">>> Volatility change: symbol=" + desc.symbol +
						" old=" + old_volatility + " new=" + desc.volatility);
			}
			long old_price = desc.lastPrice;
			double deviation = Math.min(rand.nextGaussian() / 3.0d, 1.0d);
			double delta = deviation * desc.volatility * old_price;
			long delta_l = Math.round(delta);
			boolean overflow = false;
			try {
				desc.lastPrice = Math.addExact(desc.lastPrice, delta_l);
			} catch ( ArithmeticException e ) {
				overflow = true;
				desc.lastPrice = delta_l > 0 ? MAX_PRICE : MIN_PRICE;
			}
			desc.lastPrice = Math.max(desc.lastPrice, MIN_PRICE);
			desc.lastPrice = Math.min(desc.lastPrice, MAX_PRICE);
			print(">>>  New price: " + desc.lastPrice);
			print(">>>  Old price: " + old_price);
			print(">>>  Deviation: " + deviation);
			print(">>> Volatility: " + desc.volatility);
			print(">>>      Delta: " + delta);
			print(">>>   Delta(L): " + delta_l);
			print(">>>   Overflow: " + overflow);
			return new KafkaItem(desc.lastPrice, desc.priceDecimals, rand.nextLong(), desc.volumeDecimals);
		}
		
	}
	
	private Random rand;
	private RandomUtils utils;
	
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
		
		utils = new RandomUtils(rand = new Random(conf.getInt(TradeGeneratorConfig.SEED)));
		
		Map<String, SymbolDesc> symbols = utils.randomSymbols(
				conf.getInt(TradeGeneratorConfig.SYMBOL_NUM, 1, 255),
				conf.getInt(TradeGeneratorConfig.SYMBOL_CHARS, 3, 10),
				conf.getString(TradeGeneratorConfig.SYMBOL_PREFIX),
				conf.getString(TradeGeneratorConfig.SYMBOL_SUFFIX)
			);
		List<String> symbol_list = new ArrayList<>(symbols.keySet());
		Collections.sort(symbol_list);
		print("- Symbol List ----------------------------");
		for ( String symbol : symbol_list ) {
			print(symbols.get(symbol));
		}

		int freq = 60000 / conf.getInt(TradeGeneratorConfig.TRADES_PER_MINUTE, 1, 60000);
		int freq_half = freq / 2;
		AtomicLong start_time = new AtomicLong(System.currentTimeMillis()), total_trades = new AtomicLong(0L);
		final KafkaProducer<String, KafkaItem> producer = new KafkaProducer<>(conf.getKafkaProperties());
		final String topic = conf.getString(TradeGeneratorConfig.TARGET_TOPIC);
		final CountDownLatch finished = new CountDownLatch(1);
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override public void run() {
				print("  Total time: " + (System.currentTimeMillis() - start_time.get()) + "ms");
				print("Total trades: " + total_trades.get());
				finished.countDown();
			}
		});
		
		try {
			while ( finished.getCount() > 0 ) {
				long wait = freq_half + rand.nextInt(freq);
				print("   Wait for: " + wait + "ms");
				Thread.sleep(wait);
				SymbolDesc symbol = symbols.get(symbol_list.get(rand.nextInt(symbol_list.size())));
				KafkaItem trade = utils.randomTrade(symbol);
				long time = System.currentTimeMillis();
				print("Symbol Desc: " + symbol);
				print("      Trade: " + trade);
				print("       Time: " + time);
				total_trades.incrementAndGet();
				producer.send(new ProducerRecord<>(topic, null, time, symbol.symbol, trade));
			}
		} catch ( InterruptedException e ) {
			print("Interrupted...");
		} finally {
			producer.close();
		}
		print("Exiting"); // TODO: This does not work. Move all to separate thread.
	}

}
