package ru.prolib.caelum.itemdb.kafka;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.easymock.EasyMock.*;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.easymock.IMocksControl;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import ru.prolib.caelum.core.Item;
import ru.prolib.caelum.core.IteratorStub;

@SuppressWarnings("unchecked")
public class ItemDataIteratorTest {
	@Rule public ExpectedException eex = ExpectedException.none();
	IMocksControl control;
	KafkaConsumer<String, Item> consumerMock;
	List<ConsumerRecord<String, Item>> itData;
	Iterator<ConsumerRecord<String, Item>> it;
	ItemDataIterator service;

	@Before
	public void setUp() throws Exception {
		control = createStrictControl();
		consumerMock = control.createMock(KafkaConsumer.class);
		itData = new ArrayList<>();
		it = new IteratorStub<>(itData);
		service = new ItemDataIterator(consumerMock, it, new ItemInfo("foo", 2, "bar", 0, 100L, 200L), 10L);
	}
	
	@Test
	public void testCtor_ThrowsIfItemHasNoData() {
		eex.expect(IllegalStateException.class);
		eex.expectMessage("Item has no data: topic=foo symbol=bar");
		
		service = new ItemDataIterator(consumerMock, it, new ItemInfo("foo", 2, "bar", 0, null, null), 10L);
	}
	
	@Ignore
	@Test
	public void testHasNext_() {
		fail();
	}

}
