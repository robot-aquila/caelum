package ru.prolib.caelum.itemdb.kafka;

import static org.junit.Assert.*;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.MatcherAssert.assertThat;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.Before;
import org.junit.Test;

public class KafkaItemSerdesTest {

	@Before
	public void setUp() throws Exception {
		
	}

	@Test
	public void testItemSerde() {
		Serde<KafkaItem> actual = KafkaItemSerdes.itemSerde();
		
		assertEquals(new KafkaItemSerde(), actual);
	}
	
	@Test
	public void testKeySerde() {
		Serde<String> actual = KafkaItemSerdes.keySerde();
		
		assertThat(actual, is(instanceOf(Serdes.StringSerde.class)));
	}

}
