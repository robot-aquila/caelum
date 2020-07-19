package ru.prolib.caelum.itemdb.kafka;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import static org.easymock.EasyMock.*;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.record.TimestampType;
import org.easymock.IMocksControl;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import ru.prolib.caelum.core.IItem;
import ru.prolib.caelum.core.ItemType;
import ru.prolib.caelum.core.IteratorStub;
import ru.prolib.caelum.itemdb.ItemDataResponse;

@SuppressWarnings("unchecked")
public class ItemIteratorTest {
	
	static ConsumerRecord<String, KafkaItem> CR(String t, String s, int p, long off, long time, long value, long volume) {
		return new ConsumerRecord<>(t, p, off, time, TimestampType.CREATE_TIME, 0, 0, 0, s,
				new KafkaItem(value, (byte)2, volume, (byte)0, ItemType.LONG_REGULAR));
	}
	
	static IItem ID(String s, long t, long off, long val, long vol) {
		return new Item(s, t, off, new KafkaItem(val, (byte)2, vol, (byte)0, ItemType.LONG_REGULAR));
	}
	
	@Rule public ExpectedException eex = ExpectedException.none();
	IMocksControl control;
	KafkaConsumer<String, KafkaItem> consumerMock;
	List<ConsumerRecord<String, KafkaItem>> itData;
	Iterator<ConsumerRecord<String, KafkaItem>> it;
	ItemIterator service;

	@Before
	public void setUp() throws Exception {
		control = createStrictControl();
		consumerMock = control.createMock(KafkaConsumer.class);
		itData = new ArrayList<>();
		it = new IteratorStub<>(itData);
		service = new ItemIterator(consumerMock, it, new KafkaItemInfo("foo", 2, "bar", 0, 100L, 200L), 5L, 10000L);
	}
	
	@Test
	public void testGetters() {
		assertSame(consumerMock, service.getConsumer());
		assertEquals(it, service.getSourceIterator());
		assertEquals(new KafkaItemInfo("foo", 2, "bar", 0, 100L, 200L), service.getItemInfo());
		assertEquals(5L, service.getLimit());
	}
	
	@Test
	public void testHasNext_IfFinished() {
		itData.add(CR("foo", "key", 0, 1L, 5000L, 54L, 250L)); // both should be ignored because of key
		itData.add(CR("foo", "may", 0, 2L, 5001L, 26L, 190L));
		
		assertFalse(service.hasNext());
		assertTrue(service.finished());
	}
	
	@Test
	public void testHasNext_IfHasNoData() {
		service = new ItemIterator(consumerMock, it, new KafkaItemInfo("foo", 2, "bar", 0, null, null), 10L, 10000L);
		
		assertFalse(service.hasNext());
		assertTrue(service.finished());
	}
	
	@Test
	public void testHasNext_IfClosed() {
		itData.add(CR("foo", "bar", 0, 2L, 5001L, 26L, 190L));
		service.close();
		
		assertFalse(service.hasNext());
		assertTrue(service.finished());
		assertTrue(service.closed());
	}
	
	@Test
	public void testHasNext_IfLimitReached() {
		itData.add(CR("foo", "bar", 0, 101L, 5001L, 45L, 200L));
		itData.add(CR("foo", "bar", 0, 102L, 5002L, 49L, 100L));
		itData.add(CR("foo", "bar", 0, 103L, 5003L, 43L, 500L));
		itData.add(CR("foo", "bar", 0, 104L, 5004L, 42L, 230L));
		itData.add(CR("foo", "bar", 0, 105L, 5005L, 47L, 115L));
		itData.add(CR("foo", "bar", 0, 106L, 5006L, 44L, 850L));
		
		for ( int i = 0; i < 5; i ++ ) {
			assertTrue("At #" + i, service.hasNext());
			service.next();
		}
		assertFalse(service.hasNext()); // even if it actually has more elements
		assertTrue(service.finished());
	}
	
	@Test
	public void testHasNext_IfEndOffsetReached() {
		itData.add(CR("foo", "bar", 0, 101L, 5001L, 45L, 200L));
		itData.add(CR("foo", "bar", 0, 102L, 5002L, 49L, 100L));
		itData.add(CR("foo", "bar", 0, 103L, 5003L, 43L, 500L));
		itData.add(CR("foo", "bar", 0, 104L, 5004L, 42L, 230L));
		itData.add(CR("foo", "bar", 0, 105L, 5005L, 47L, 115L));
		itData.add(CR("foo", "bar", 0, 106L, 5006L, 44L, 850L));
		// last offset + 1 !!!
		service = new ItemIterator(consumerMock, it, new KafkaItemInfo("foo", 2, "bar", 0, 100L, 104L), 5L, 10000L);
		
		for ( int i = 0; i < 3; i ++ ) {
			assertTrue("At #" + i, service.hasNext());
			service.next();
		}
		assertFalse(service.hasNext());
		assertTrue(service.finished());
	}
	
	@Test
	public void testHasNext_IfEndTimeReached() {
		itData.add(CR("foo", "bar", 0, 101L, 5001L, 45L, 200L));
		itData.add(CR("foo", "bar", 0, 102L, 5002L, 49L, 100L));
		itData.add(CR("foo", "bar", 0, 103L, 5003L, 43L, 500L));
		itData.add(CR("foo", "bar", 0, 104L, 5004L, 42L, 230L));
		itData.add(CR("foo", "bar", 0, 105L, 5005L, 47L, 115L));
		itData.add(CR("foo", "bar", 0, 106L, 5006L, 44L, 850L));
		service = new ItemIterator(consumerMock, it, new KafkaItemInfo("foo", 2, "bar", 0, 100L, 200L), 5L, 5004L);
		
		for ( int i = 0; i < 3; i ++ ) {
			assertTrue("At #" + i, service.hasNext());
			service.next();
		}
		assertFalse(service.hasNext());
		assertTrue(service.finished());
	}
	
	@Test
	public void testHasNext_ThrowsIfPartitionChanged() {
		itData.add(CR("foo", "bar", 1, 101L, 5001L, 45L, 200L));
		itData.add(CR("foo", "bar", 0, 102L, 5002L, 49L, 100L));
		itData.add(CR("foo", "bar", 0, 103L, 5003L, 43L, 500L));
		itData.add(CR("foo", "bar", 0, 104L, 5004L, 42L, 230L));
		itData.add(CR("foo", "bar", 0, 105L, 5005L, 47L, 115L));
		itData.add(CR("foo", "bar", 0, 106L, 5006L, 44L, 850L));
		eex.expect(IllegalStateException.class);
		eex.expectMessage("Partition changed: expected=0 actual=1");
		
		service.hasNext();
	}
	
	@Test
	public void testNext_ThrowsIfClosed() {
		itData.add(CR("foo", "bar", 0, 2L, 5001L, 26L, 190L));
		service.close();
		eex.expect(NoSuchElementException.class);
		
		service.next();
	}
	
	@Test
	public void testNext_ThrowsIfFinished() {
		itData.add(CR("foo", "key", 0, 1L, 5000L, 54L, 250L)); // both should be ignored because of key
		itData.add(CR("foo", "may", 0, 2L, 5001L, 26L, 190L));
		eex.expect(NoSuchElementException.class);
		
		service.next();
	}
	
	@Test
	public void testNext_ThrowsIfHasNoData() {
		service = new ItemIterator(consumerMock, it, new KafkaItemInfo("foo", 2, "bar", 0, null, null), 10L, 10000L);
		eex.expect(NoSuchElementException.class);
		
		service.next();
	}
	
	@Test
	public void testNext_ThrowsIfLimitReached() {
		itData.add(CR("foo", "bar", 0, 101L, 5001L, 45L, 200L));
		itData.add(CR("foo", "bar", 0, 102L, 5002L, 49L, 100L));
		itData.add(CR("foo", "bar", 0, 103L, 5003L, 43L, 500L));
		itData.add(CR("foo", "bar", 0, 104L, 5004L, 42L, 230L));
		itData.add(CR("foo", "bar", 0, 105L, 5005L, 47L, 115L));
		itData.add(CR("foo", "bar", 0, 106L, 5006L, 44L, 850L));
		for ( int i = 0; i < 5; i ++ ) {
			assertTrue("At #" + i, service.hasNext());
			service.next();
		}
		eex.expect(NoSuchElementException.class);
		
		service.next();
	}
	
	@Test
	public void testNext_ThrowsIfEndOfData() {
		itData.add(CR("foo", "bar", 0, 101L, 5001L, 45L, 200L));
		itData.add(CR("foo", "bar", 0, 102L, 5002L, 49L, 100L));
		service.next();
		service.next();
		eex.expect(NoSuchElementException.class);
		
		service.next();
	}
	
	@Test
	public void testNext_ThrowsIfEndOffsetReached() {
		itData.add(CR("foo", "bar", 0, 101L, 5001L, 45L, 200L));
		itData.add(CR("foo", "bar", 0, 102L, 5002L, 49L, 100L));
		itData.add(CR("foo", "bar", 0, 103L, 5003L, 43L, 500L));
		itData.add(CR("foo", "bar", 0, 104L, 5004L, 42L, 230L));
		itData.add(CR("foo", "bar", 0, 105L, 5005L, 47L, 115L));
		itData.add(CR("foo", "bar", 0, 106L, 5006L, 44L, 850L));
		// last offset + 1 !!!
		service = new ItemIterator(consumerMock, it, new KafkaItemInfo("foo", 2, "bar", 0, 100L, 104L), 5L, 10000L);
		for ( int i = 0; i < 3; i ++ ) {
			assertTrue("At #" + i, service.hasNext());
			service.next();
		}
		eex.expect(NoSuchElementException.class);
		
		service.next();
	}
	
	@Test
	public void testNext_ThrowsIfEndTimeReached() {
		itData.add(CR("foo", "bar", 0, 101L, 5001L, 45L, 200L));
		itData.add(CR("foo", "bar", 0, 102L, 5002L, 49L, 100L));
		itData.add(CR("foo", "bar", 0, 103L, 5003L, 43L, 500L));
		itData.add(CR("foo", "bar", 0, 104L, 5004L, 42L, 230L));
		itData.add(CR("foo", "bar", 0, 105L, 5005L, 47L, 115L));
		itData.add(CR("foo", "bar", 0, 106L, 5006L, 44L, 850L));
		service = new ItemIterator(consumerMock, it, new KafkaItemInfo("foo", 2, "bar", 0, 100L, 200L), 5L, 5004L);
		for ( int i = 0; i < 3; i ++ ) {
			service.next();
		}
		eex.expect(NoSuchElementException.class);

		service.next();
	}
	
	@Test
	public void testNext_ThrowsIfPartitionChanged() {
		itData.add(CR("foo", "bar", 1, 101L, 5001L, 45L, 200L));
		itData.add(CR("foo", "bar", 0, 102L, 5002L, 49L, 100L));
		itData.add(CR("foo", "bar", 0, 103L, 5003L, 43L, 500L));
		itData.add(CR("foo", "bar", 0, 104L, 5004L, 42L, 230L));
		itData.add(CR("foo", "bar", 0, 105L, 5005L, 47L, 115L));
		itData.add(CR("foo", "bar", 0, 106L, 5006L, 44L, 850L));
		eex.expect(IllegalStateException.class);
		eex.expectMessage("Partition changed: expected=0 actual=1");
		
		service.next();
	}
	
	@Test
	public void testNext_Iterate() {
		service = new ItemIterator(consumerMock, it, new KafkaItemInfo("foo", 2, "bar", 0, 100L, 200L), 25L, 10000L);
		itData.add(CR("foo", "bar", 0, 101L, 5001L, 45L, 200L));
		itData.add(CR("foo", "bar", 0, 102L, 5002L, 49L, 100L));
		itData.add(CR("foo", "bar", 0, 103L, 5003L, 43L, 500L));
		itData.add(CR("foo", "bar", 0, 104L, 5004L, 42L, 230L));
		itData.add(CR("foo", "bar", 0, 105L, 5005L, 47L, 115L));
		itData.add(CR("foo", "bar", 0, 106L, 5006L, 44L, 850L));

		List<IItem> actual = new ArrayList<>();
		for ( int i = 0; i < 6; i ++ ) {
			assertTrue("At #" + i, service.hasNext());
			actual.add(service.next());
		}
		assertFalse(service.hasNext());
		
		List<IItem> expected = Arrays.asList(
				ID("bar", 5001L, 101L, 45L, 200L),
				ID("bar", 5002L, 102L, 49L, 100L),
				ID("bar", 5003L, 103L, 43L, 500L),
				ID("bar", 5004L, 104L, 42L, 230L),
				ID("bar", 5005L, 105L, 47L, 115L),
				ID("bar", 5006L, 106L, 44L, 850L)
			);
		assertEquals(expected, actual);
	}
	
	@Test
	public void testGetMetaData() {
		String magic = DigestUtils.md5Hex("bar:100:2"); // symbol : beg_offset : num_partitions
		itData.add(CR("foo", "bar", 0, 101L, 5001L, 45L, 200L));
		itData.add(CR("foo", "bar", 0, 102L, 5002L, 49L, 100L));
		itData.add(CR("foo", "bar", 0, 103L, 5003L, 43L, 500L));
		
		assertEquals(new ItemDataResponse(  0, magic), service.getMetaData());
		service.next();
		assertEquals(new ItemDataResponse(102, magic), service.getMetaData());
		service.next();
		assertEquals(new ItemDataResponse(103, magic), service.getMetaData());
		service.next();
		assertEquals(new ItemDataResponse(103, magic), service.getMetaData());
		assertEquals(new ItemDataResponse(103, magic), service.getMetaData());
	}
	
	@Test
	public void testGetMetaData_ThrowsIfClosed() {
		itData.add(CR("foo", "bar", 0, 101L, 5001L, 45L, 200L));
		itData.add(CR("foo", "bar", 0, 102L, 5002L, 49L, 100L));
		itData.add(CR("foo", "bar", 0, 103L, 5003L, 43L, 500L));
		service.next();
		service.close();
		eex.expect(IllegalStateException.class);
		eex.expectMessage("Iterator already closed");
		
		service.getMetaData();
	}
	
	@Test
	public void testClose() {
		service.close();
		
		assertTrue(service.closed());
	}

}
