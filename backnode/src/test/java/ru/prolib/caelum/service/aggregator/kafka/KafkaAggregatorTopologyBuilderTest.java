package ru.prolib.caelum.service.aggregator.kafka;

import static org.junit.Assert.*;
import static org.easymock.EasyMock.*;

import java.time.Instant;

import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.WindowedSerdes;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.test.TestRecord;
import org.apache.log4j.BasicConfigurator;
import org.easymock.IMocksControl;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import ru.prolib.caelum.lib.HostInfo;
import ru.prolib.caelum.lib.Interval;
import ru.prolib.caelum.lib.ItemType;
import ru.prolib.caelum.lib.TupleType;
import ru.prolib.caelum.lib.kafka.KafkaItem;
import ru.prolib.caelum.lib.kafka.KafkaItemSerdes;
import ru.prolib.caelum.lib.kafka.KafkaTuple;
import ru.prolib.caelum.lib.kafka.KafkaTupleSerdes;
import ru.prolib.caelum.service.GeneralConfig;

public class KafkaAggregatorTopologyBuilderTest {
	private static final byte DEFAULT_DECIMALS = 2;
	private static final byte DEFAULT_VOL_DECIMALS = 0;
	
	static KafkaItem KI(int value, byte decimals, int volume, byte vol_decimals) {
		return new KafkaItem(value, decimals, volume, vol_decimals, ItemType.LONG_REGULAR);
	}
	
	static KafkaItem KI(int value, int volume) {
		return KI(value, DEFAULT_DECIMALS, volume, DEFAULT_VOL_DECIMALS);
	}
	
	static KafkaTuple KT(int open, int high, int low, int close, byte decimals, int volume, byte vol_decimals) {
		return new KafkaTuple(open, high, low, close, decimals,
				(long) volume, null, vol_decimals, TupleType.LONG_REGULAR);
	}
	
	static KafkaTuple KT(int open, int high, int low, int close, int volume) {
		return KT(open, high, low, close, DEFAULT_DECIMALS, volume, DEFAULT_VOL_DECIMALS);
	}
	
	static TestRecord<Windowed<String>, KafkaTuple>
		TR(String key, long tuple_time, long record_time, KafkaTuple tuple)
	{
		return new TestRecord<>(new Windowed<String>(key, new TimeWindow(tuple_time, Long.MAX_VALUE)),
				tuple,
				Instant.ofEpochMilli(record_time));
	}
	
	@BeforeClass
	public static void setUpBeforeClass() {
		BasicConfigurator.resetConfiguration();
		BasicConfigurator.configure();
	}
	
	IMocksControl control;
	GeneralConfig gconfMock;
	Interval interval;
	KafkaAggregatorConfig config;
	KafkaAggregatorTopologyBuilder service;
	TopologyTestDriver testDriver;
	TestInputTopic<String, KafkaItem> items;
	TestOutputTopic<Windowed<String>, KafkaTuple> tuples;
	ReadOnlyWindowStore<String, KafkaTuple> store;

	@Before
	public void setUp() throws Exception {
		control = createStrictControl();
		gconfMock = control.createMock(GeneralConfig.class);
		config = new KafkaAggregatorConfig(interval = Interval.M5, gconfMock);
		service = new KafkaAggregatorTopologyBuilder();
	}
	
	@After
	public void tearDown() {
		if ( testDriver != null ) {
			testDriver.close();
			testDriver = null;
		}
		items = null;
		tuples = null;
		store = null;
	}

	@Test
	public void testBuildTopology_TopologyShouldBeOk() {
		expect(gconfMock.getAggregatorKafkaApplicationIdPrefix()).andStubReturn("test-app-");
		expect(gconfMock.getKafkaBootstrapServers()).andStubReturn("dummy:123");
		expect(gconfMock.getAggregatorKafkaStorePrefix()).andStubReturn("test-store-");
		expect(gconfMock.getItemsTopicName()).andStubReturn("test-items");
		expect(gconfMock.getAggregatorKafkaTargetTopicPrefix()).andStubReturn("test-tuples-");
		expect(gconfMock.getAggregatorKafkaStoreRetentionTime()).andStubReturn(561826883L);
		expect(gconfMock.getKafkaStateDir()).andStubReturn("/tmp/kafka-streams");
		expect(gconfMock.getAggregatorKafkaNumStreamThreads()).andStubReturn(2);
		expect(gconfMock.getHttpInfo()).andStubReturn(new HostInfo("bambr", 1256));
		expect(gconfMock.getAggregatorKafkaLingerMs()).andStubReturn(5L);
		control.replay();
		
		testDriver = new TopologyTestDriver(service.buildTopology(config), config.getKafkaStreamsProperties());
		items = testDriver.createInputTopic("test-items",
				KafkaItemSerdes.keySerde().serializer(),
				KafkaItemSerdes.itemSerde().serializer());
		tuples = testDriver.createOutputTopic("test-tuples-m5",
				WindowedSerdes.timeWindowedSerdeFrom(String.class).deserializer(),
				KafkaTupleSerdes.tupleSerde().deserializer());
		store = testDriver.getWindowStore("test-store-m5");

		final long time_adv = 300000;
		final String key = "foo@bar";
		KafkaTuple tuple = null;
		
		long rec_time = time_adv * 0, tup_time = rec_time;
		items.pipeInput(key, KI(250, 1000), rec_time);
		assertEquals(TR(key, tup_time, rec_time, tuple = KT(250, 250, 250, 250, 1000)), tuples.readRecord());
		assertEquals(tuple, store.fetch(key, tup_time));

		items.pipeInput(key, KI(245,  500), rec_time);
		assertEquals(TR(key, tup_time, rec_time, tuple = KT(250, 250, 245, 245, 1500)), tuples.readRecord());
		assertEquals(tuple, store.fetch(key, tup_time));
		
		rec_time = time_adv * 0 + 1000;
		items.pipeInput(key, KI(240,  100), rec_time);
		assertEquals(TR(key, tup_time, rec_time, tuple = KT(250, 250, 240, 240, 1600)), tuples.readRecord());
		assertEquals(tuple, store.fetch(key, tup_time));
		
		rec_time = time_adv * 0 + 150000;
		items.pipeInput(key, KI(251, 200), rec_time);
		assertEquals(TR(key, tup_time, rec_time, tuple = KT(250, 251, 240, 251, 1800)), tuples.readRecord());
		assertEquals(tuple, store.fetch(key, tup_time));
		
		rec_time = time_adv * 1 + 5000;
		tup_time = time_adv * 1;
		items.pipeInput(key, KI(239, 1000), rec_time);
		assertEquals(TR(key, tup_time, rec_time, tuple = KT(239, 239, 239, 239, 1000)), tuples.readRecord());
		assertEquals(tuple, store.fetch(key, tup_time));
		
		rec_time = time_adv * 1 + 10000;
		items.pipeInput(key, KI(220, 1000), rec_time);
		assertEquals(TR(key, tup_time, rec_time, tuple = KT(239, 239, 220, 220, 2000)), tuples.readRecord());
		assertEquals(tuple, store.fetch(key, tup_time));
	}
	
	@Test
	public void testHashCode() {
		assertEquals(668915632, service.hashCode());
	}
	
	@SuppressWarnings("unlikely-arg-type")
	@Test
	public void testEquals() {
		assertTrue(service.equals(service));
		assertTrue(service.equals(new KafkaAggregatorTopologyBuilder()));
		assertFalse(service.equals(null));
		assertFalse(service.equals(this));
	}

}
