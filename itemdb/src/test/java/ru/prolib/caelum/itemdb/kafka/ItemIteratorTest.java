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
import org.apache.log4j.BasicConfigurator;
import org.easymock.IMocksControl;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

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
	
	@BeforeClass
	public static void setUpBeforeClass() {
		BasicConfigurator.resetConfiguration();
		BasicConfigurator.configure();
	}
	
	IMocksControl control;
	KafkaItemInfo itemInfo;
	KafkaConsumer<String, KafkaItem> consumerMock;
	List<ConsumerRecord<String, KafkaItem>> itData;
	Iterator<ConsumerRecord<String, KafkaItem>> it, itMock;
	ItemIterator service;

	@Before
	public void setUp() throws Exception {
		control = createStrictControl();
		consumerMock = control.createMock(KafkaConsumer.class);
		itData = new ArrayList<>();
		it = new IteratorStub<>(itData);
		itMock = control.createMock(Iterator.class);
		itemInfo = new KafkaItemInfo("foo", 2, "bar", 0, 100L, 200L);
		service = new ItemIterator(consumerMock, it, itemInfo, 5, 99L, 10000L);
	}
	
	@Test
	public void testGetters() {
		assertSame(consumerMock, service.getConsumer());
		assertEquals(it, service.getSourceIterator());
		assertEquals(new KafkaItemInfo("foo", 2, "bar", 0, 100L, 200L), service.getItemInfo());
		assertEquals(5, service.getLimit());
		assertEquals(Long.valueOf(   99L), service.getStartTime());
		assertEquals(Long.valueOf(10000L), service.getEndTime());
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
		itemInfo = new KafkaItemInfo("foo", 2, "bar", 0, null, null);
		service = new ItemIterator(consumerMock, it, itemInfo, 10, null, null);
		
		assertFalse(service.hasNext());
		assertTrue(service.finished());
	}
	
	@Test
	public void testHasNext_IfClosed() {
		itData.add(CR("foo", "bar", 0, 2L, 5001L, 26L, 190L));
		control.resetToNice();
		expect(consumerMock.position(anyObject())).andStubReturn(100L);
		control.replay();
		
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
		itemInfo = new KafkaItemInfo("foo", 2, "bar", 0, 100L, 104L);
		service = new ItemIterator(consumerMock, it, itemInfo, 5, 99L, 10000L);
		
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
		itemInfo = new KafkaItemInfo("foo", 2, "bar", 0, 100L, 200L);
		service = new ItemIterator(consumerMock, it, itemInfo, 5, 99L, 5004L);
		
		for ( int i = 0; i < 3; i ++ ) {
			assertTrue("At #" + i, service.hasNext());
			service.next();
		}
		assertFalse(service.hasNext());
		assertTrue(service.finished());
	}
	
	@Test
	public void testHasNext_ShouldIgnoreEndTimeCheckingIfEndTimeIsNull() {
		itData.add(CR("foo", "bar", 0, 101L, 5001L, 45L, 200L));
		itData.add(CR("foo", "bar", 0, 102L, 5002L, 49L, 100L));
		itData.add(CR("foo", "bar", 0, 103L, 5003L, 43L, 500L));
		itData.add(CR("foo", "bar", 0, 104L, 5004L, 42L, 230L));
		itData.add(CR("foo", "bar", 0, 105L, 5005L, 47L, 115L));
		itData.add(CR("foo", "bar", 0, 106L, 5006L, 44L, 850L));
		itemInfo = new KafkaItemInfo("foo", 2, "bar", 0, 100L, 1000L);
		service = new ItemIterator(consumerMock, it, itemInfo, 1000, 99L, null);

		for ( int i = 0; i < 6; i ++ ) {
			assertTrue("At#" + i, service.hasNext());
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
		
		IllegalStateException e = assertThrows(IllegalStateException.class, () -> service.hasNext());
		assertEquals("Partition changed: expected=0 actual=1", e.getMessage());
	}
	
	@Test
	public void testHasNext_ShouldProcessNoSuchElementExceptionThrownByUnderlyingIterator() {
		itemInfo = new KafkaItemInfo("foo", 2, "bar", 0, 100L, 200L);
		service = new ItemIterator(consumerMock, itMock, itemInfo, 5, 99L, 10000L);
		expect(itMock.hasNext()).andReturn(true);
		expect(itMock.next()).andThrow(new NoSuchElementException());
		control.replay();
		
		assertFalse(service.hasNext());
		
		control.verify();
	}
	
	@Test
	public void testHasNext_ShouldSkipRecordsWithTimePriorToStartTime() {
		itemInfo = new KafkaItemInfo("foo", 2, "bar", 0, 100L, 200L);
		service = new ItemIterator(consumerMock, it, itemInfo, 5, 9000L, 10000L);
		itData.add(CR("foo", "bar", 0, 101L, 8995L, 45L, 200L));
		itData.add(CR("foo", "bar", 0, 102L, 8996L, 43L, 130L));
		itData.add(CR("foo", "bar", 0, 103L, 8997L, 41L, 250L));
		itData.add(CR("foo", "bar", 0, 104L, 8998L, 42L, 400L));
		itData.add(CR("foo", "bar", 0, 105L, 8999L, 48L, 300L));
		control.replay();
		
		assertFalse(service.hasNext());
		
		control.verify();
	}
	
	@Test
	public void testHasNext_ShouldIgnoreStartTimeCheckingIfNotDefined() {
		itemInfo = new KafkaItemInfo("foo", 2, "bar", 0, 100L, 200L);
		service = new ItemIterator(consumerMock, it, itemInfo, 5, null, 10000L);
		itData.add(CR("foo", "bar", 0, 101L, 8995L, 45L, 200L));
		itData.add(CR("foo", "bar", 0, 102L, 8996L, 43L, 130L));
		itData.add(CR("foo", "bar", 0, 103L, 8997L, 41L, 250L));
		itData.add(CR("foo", "bar", 0, 104L, 8998L, 42L, 400L));
		itData.add(CR("foo", "bar", 0, 105L, 8999L, 48L, 300L));
		control.replay();
		
		assertTrue(service.hasNext()); service.next();
		assertTrue(service.hasNext()); service.next();
		assertTrue(service.hasNext()); service.next();
		assertTrue(service.hasNext()); service.next();
		assertTrue(service.hasNext()); service.next();
		assertFalse(service.hasNext());
		
		control.verify();
	}
	
	@Test
	public void testNext_ThrowsIfClosed() {
		itData.add(CR("foo", "bar", 0, 2L, 5001L, 26L, 190L));
		control.resetToNice();
		expect(consumerMock.position(anyObject())).andStubReturn(100L);
		control.replay();
		service.close();
		
		assertThrows(NoSuchElementException.class, () -> service.next());
	}
	
	@Test
	public void testNext_ThrowsIfFinished() {
		itData.add(CR("foo", "key", 0, 1L, 5000L, 54L, 250L)); // both should be ignored because of key
		itData.add(CR("foo", "may", 0, 2L, 5001L, 26L, 190L));
		
		assertThrows(NoSuchElementException.class, () -> service.next());
	}
	
	@Test
	public void testNext_ThrowsIfHasNoData() {
		itemInfo = new KafkaItemInfo("foo", 2, "bar", 0, null, null);
		service = new ItemIterator(consumerMock, it, itemInfo, 10, 99L, 10000L);
		
		assertThrows(NoSuchElementException.class, () -> service.next());
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
		
		assertThrows(NoSuchElementException.class, () -> service.next());
	}
	
	@Test
	public void testNext_ThrowsIfEndOfData() {
		itData.add(CR("foo", "bar", 0, 101L, 5001L, 45L, 200L));
		itData.add(CR("foo", "bar", 0, 102L, 5002L, 49L, 100L));
		service.next();
		service.next();
		
		assertThrows(NoSuchElementException.class, () -> service.next());
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
		itemInfo = new KafkaItemInfo("foo", 2, "bar", 0, 100L, 104L);
		service = new ItemIterator(consumerMock, it, itemInfo, 5, 99L, 10000L);
		for ( int i = 0; i < 3; i ++ ) {
			assertTrue("At #" + i, service.hasNext());
			service.next();
		}
		
		assertThrows(NoSuchElementException.class, () -> service.next());
	}
	
	@Test
	public void testNext_ThrowsIfEndTimeReached() {
		itData.add(CR("foo", "bar", 0, 101L, 5001L, 45L, 200L));
		itData.add(CR("foo", "bar", 0, 102L, 5002L, 49L, 100L));
		itData.add(CR("foo", "bar", 0, 103L, 5003L, 43L, 500L));
		itData.add(CR("foo", "bar", 0, 104L, 5004L, 42L, 230L));
		itData.add(CR("foo", "bar", 0, 105L, 5005L, 47L, 115L));
		itData.add(CR("foo", "bar", 0, 106L, 5006L, 44L, 850L));
		itemInfo = new KafkaItemInfo("foo", 2, "bar", 0, 100L, 200L);
		service = new ItemIterator(consumerMock, it, itemInfo, 5, 99L, 5004L);
		for ( int i = 0; i < 3; i ++ ) {
			service.next();
		}

		assertThrows(NoSuchElementException.class, () -> service.next());
	}
	
	@Test
	public void testNext_ShouldIgnoreEndTimeCheckingIfEndTimeIsNull() {
		itData.add(CR("foo", "bar", 0, 101L, 5001L, 45L, 200L));
		itData.add(CR("foo", "bar", 0, 102L, 5002L, 49L, 100L));
		itData.add(CR("foo", "bar", 0, 103L, 5003L, 43L, 500L));
		itData.add(CR("foo", "bar", 0, 104L, 5004L, 42L, 230L));
		itData.add(CR("foo", "bar", 0, 105L, 5005L, 47L, 115L));
		itData.add(CR("foo", "bar", 0, 106L, 5006L, 44L, 850L));
		itemInfo = new KafkaItemInfo("foo", 2, "bar", 0, 100L, 200L);
		service = new ItemIterator(consumerMock, it, itemInfo, 1000, null, null);
		
		List<IItem> actual = new ArrayList<>();
		while ( service.hasNext() ) {
			actual.add(service.next());
		}
		
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
	public void testNext_ThrowsIfPartitionChanged() {
		itData.add(CR("foo", "bar", 1, 101L, 5001L, 45L, 200L));
		itData.add(CR("foo", "bar", 0, 102L, 5002L, 49L, 100L));
		itData.add(CR("foo", "bar", 0, 103L, 5003L, 43L, 500L));
		itData.add(CR("foo", "bar", 0, 104L, 5004L, 42L, 230L));
		itData.add(CR("foo", "bar", 0, 105L, 5005L, 47L, 115L));
		itData.add(CR("foo", "bar", 0, 106L, 5006L, 44L, 850L));
		
		IllegalStateException e = assertThrows(IllegalStateException.class, () -> service.next());
		assertEquals("Partition changed: expected=0 actual=1", e.getMessage());
	}
	
	@Test
	public void testNext_ShouldProcessNoSuchElementExceptionThrownByUnderlyingIterator() {
		itemInfo = new KafkaItemInfo("foo", 2, "bar", 0, 100L, 200L);
		service = new ItemIterator(consumerMock, itMock, itemInfo, 5, 99L, 10000L);
		expect(itMock.hasNext()).andReturn(true);
		expect(itMock.next()).andThrow(new NoSuchElementException());
		control.replay();
		
		assertThrows(NoSuchElementException.class, () -> service.next());
		
		control.verify();
	}
	
	@Test
	public void testNext_ShouldSkipRecordsWithTimePriorToStartTime() {
		itemInfo = new KafkaItemInfo("foo", 2, "bar", 0, 100L, 200L);
		service = new ItemIterator(consumerMock, it, itemInfo, 5, 9000L, 10000L);
		itData.add(CR("foo", "bar", 0, 101L, 9001L, 45L, 200L));
		itData.add(CR("foo", "bar", 0, 102L, 8996L, 43L, 130L));
		itData.add(CR("foo", "bar", 0, 103L, 9002L, 41L, 250L));
		itData.add(CR("foo", "bar", 0, 104L, 8998L, 42L, 400L));
		itData.add(CR("foo", "bar", 0, 105L, 9003L, 48L, 300L));
		control.replay();
		
		assertTrue(service.hasNext());
		assertEquals(ID("bar", 9001L, 101L, 45L, 200L), service.next());
		assertTrue(service.hasNext());
		assertEquals(ID("bar", 9002L, 103L, 41L, 250L), service.next());
		assertTrue(service.hasNext());
		assertEquals(ID("bar", 9003L, 105L, 48L, 300L), service.next());
		assertFalse(service.hasNext());
		
		control.verify();
	}
	
	@Test
	public void testNext_ShouldIgnoreStartTimeCheckingIfNotDefined() {
		itemInfo = new KafkaItemInfo("foo", 2, "bar", 0, 100L, 200L);
		service = new ItemIterator(consumerMock, it, itemInfo, 5, null, 10000L);
		itData.add(CR("foo", "bar", 0, 101L, 9001L, 45L, 200L));
		itData.add(CR("foo", "bar", 0, 102L, 8996L, 43L, 130L));
		itData.add(CR("foo", "bar", 0, 103L, 9002L, 41L, 250L));
		itData.add(CR("foo", "bar", 0, 104L, 8998L, 42L, 400L));
		itData.add(CR("foo", "bar", 0, 105L, 9003L, 48L, 300L));
		control.replay();
		
		List<IItem> actual = new ArrayList<>();
		for ( int i = 0; i < 5; i ++ ) {
			assertTrue("At #" + i, service.hasNext());
			actual.add(service.next());
		}
		assertFalse(service.hasNext());
		
		List<IItem> expected = Arrays.asList(
				ID("bar", 9001L, 101L, 45L, 200L),
				ID("bar", 8996L, 102L, 43L, 130L),
				ID("bar", 9002L, 103L, 41L, 250L),
				ID("bar", 8998L, 104L, 42L, 400L),
				ID("bar", 9003L, 105L, 48L, 300L)
			);
		assertEquals(expected, actual);
	}
	
	@Test
	public void testNext_Iterate() {
		itemInfo = new KafkaItemInfo("foo", 2, "bar", 0, 100L, 200L);
		service = new ItemIterator(consumerMock, it, itemInfo, 25, 99L, 10000L);
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
		control.resetToNice();
		expect(consumerMock.position(anyObject())).andStubReturn(100L);
		control.replay();
		service.close();
		
		IllegalStateException e = assertThrows(IllegalStateException.class, () -> service.getMetaData());
		assertEquals("Iterator already closed", e.getMessage());
	}
	
	@Test
	public void testClose() {
		consumerMock.close();
		control.replay();
		
		service.close();
		
		control.verify();
		assertTrue(service.closed());
	}

}
