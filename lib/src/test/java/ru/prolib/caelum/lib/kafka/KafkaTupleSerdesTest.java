package ru.prolib.caelum.lib.kafka;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
import static org.hamcrest.MatcherAssert.assertThat;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.Before;
import org.junit.Test;

public class KafkaTupleSerdesTest {

	@Before
	public void setUp() throws Exception {
		
	}

	@Test
	public void testTupleSerde() {
		Serde<KafkaTuple> actual = KafkaTupleSerdes.tupleSerde();
		
		assertEquals(new KafkaTupleSerde(), actual);
	}
	
	@Test
	public void testKeySerde() {
		Serde<String> actual = KafkaTupleSerdes.keySerde();
		
		assertThat(actual, is(instanceOf(Serdes.StringSerde.class)));
	}

}
