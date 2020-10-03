package ru.prolib.caelum.service.itesym;

import static org.junit.Assert.*;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.easymock.EasyMock.*;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.log4j.BasicConfigurator;
import org.easymock.IMocksControl;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import ru.prolib.caelum.service.ExtensionState;
import ru.prolib.caelum.service.ExtensionStatus;
import ru.prolib.caelum.service.ICaelum;

@SuppressWarnings("unchecked")
public class ItesymTest {
	static final TopicPartition part1 = new TopicPartition("foobar", 0);
	static final TopicPartition part2 = new TopicPartition("foobar", 1);
	
	@BeforeClass
	public static void setUpBeforeClass() {
		BasicConfigurator.resetConfiguration();
		BasicConfigurator.configure();
	}
	
	static Set<String> toSet(String... args) {
		Set<String> r = new HashSet<>();
		for ( String s : args ) {
			r.add(s);
		}
		return r;
	}
	
	static ConsumerRecord<String, byte[]> CR(String key, TopicPartition part) {
		byte[] unused = new byte[8];
		return new ConsumerRecord<>(part.topic(), part.partition(), 0, key, unused);
	}
	
	static ConsumerRecords<String, byte[]> CRS(ConsumerRecord<String, byte[]> ...records) {
		Map<TopicPartition, List<ConsumerRecord<String, byte[]>>> map = new HashMap<>();
		for ( ConsumerRecord<String, byte[]> record : records ) {
			TopicPartition part = new TopicPartition(record.topic(), record.partition());
			List<ConsumerRecord<String, byte[]>> list = map.get(part);
			if ( list == null ) {
				list = new ArrayList<>();
				map.put(part, list);
			}
			list.add(record);
		}
		return new ConsumerRecords<>(map);
	}
	
	IMocksControl control;
	KafkaConsumer<String, byte[]> consumerMock;
	ICaelum caelumMock;
	Runnable runnableMock;
	Thread thread;
	AtomicReference<Thread> thread_ref;
	AtomicBoolean close, clear, started, stopped;
	Itesym service;

	@Before
	public void setUp() throws Exception {
		control = createStrictControl();
		consumerMock = control.createMock(KafkaConsumer.class);
		caelumMock = control.createMock(ICaelum.class);
		runnableMock = control.createMock(Runnable.class);
		thread = new Thread(runnableMock);
		thread_ref = new AtomicReference<>();
		close = new AtomicBoolean();
		clear = new AtomicBoolean();
		started = new AtomicBoolean();
		stopped = new AtomicBoolean();
		service = new Itesym("bob", consumerMock, "rob", caelumMock, 50L, 100L,
				thread_ref, close, clear, started, stopped);
		thread_ref.set(thread);
	}
	
	@Test
	public void testGetters() {
		assertEquals("bob", service.getGroupId());
		assertSame(consumerMock, service.getConsumer());
		assertEquals("rob", service.getSourceTopic());
		assertSame(caelumMock, service.getCaelum());
		assertEquals(50L, service.getPollTimeout());
		assertEquals(100L, service.getShutdownTimeout());
		assertSame(thread, service.getThread());
	}
	
	@Test
	public void testGetStatus() {
		assertEquals(new ExtensionStatus(ExtensionState.CREATED, null), service.getStatus());
	}
	
	@Test
	public void testClear_ShouldSkipIfAlreadyRequested() {
		clear.set(true);
		control.replay();
		
		service.clear();
		
		control.verify();
	}
	
	@Test
	public void testClear_ShouldMakeRequest() {
		assertFalse(clear.get());
		consumerMock.wakeup();
		control.replay();
		
		service.clear();
		
		control.verify();
		assertTrue(clear.get());
	}
	
	@Test
	public void testStart_ShouldStartIfNotStarted() throws Exception {
		assertFalse(started.get());
		runnableMock.run();
		control.replay();
		
		service.start();

		assertTrue(thread.isAlive());
		thread.join(1000L);
		assertFalse(thread.isAlive());
		control.verify();
		assertTrue(started.get());
	}
	
	@Test
	public void testStart_ShouldSkipIfStarted() {
		started.set(true);
		control.replay();
		
		service.start();
		
		control.verify();
		assertFalse(thread.isAlive());
	}
	
	@Test
	public void testStop_ShouldStopIfNotStopped() throws Exception {
		CountDownLatch _started = new CountDownLatch(1), _go = new CountDownLatch(1), _finished = new CountDownLatch(1);
		assertFalse(stopped.get());
		assertFalse(close.get());
		consumerMock.wakeup();
		expectLastCall().andAnswer(() -> { _go.countDown(); return null; });
		thread = new Thread(() -> {
			_started.countDown();
			try {
				assertTrue(_go.await(1, TimeUnit.SECONDS));
			} catch ( InterruptedException e ) { }
			assertTrue(stopped.get());
			assertTrue(close.get());
			_finished.countDown();
		});
		control.replay();
		service = new Itesym("bob", consumerMock, "rob", caelumMock, 50L, 100L,
				thread_ref, close, clear, started, stopped);
		thread_ref.set(thread);
		thread.start();
		assertTrue(_started.await(500, TimeUnit.MILLISECONDS));
		
		service.stop();
		
		assertTrue(_finished.await(1, TimeUnit.SECONDS));
		control.verify();
	}
	
	@Test
	public void testStop_ShouldWaitForTermination() throws Exception {
		CountDownLatch _started = new CountDownLatch(1), _go = new CountDownLatch(1), _finished = new CountDownLatch(1);
		consumerMock.wakeup();
		thread = new Thread(() -> {
			_started.countDown();
			try {
				assertTrue(_go.await(1, TimeUnit.SECONDS));
			} catch ( InterruptedException e ) { }
			_finished.countDown();
		});
		control.replay();
		service = new Itesym("bob", consumerMock, "rob", caelumMock, 50L, 100L,
				thread_ref, close, clear, started, stopped);
		thread_ref.set(thread);
		thread.start();
		assertTrue(_started.await(500, TimeUnit.MILLISECONDS));
		
		service.stop();
		
		_go.countDown();
		assertTrue(_finished.await(1, TimeUnit.SECONDS));
	}
	
	@Test
	public void testRun_ShouldAvoidDuplicatesInSeparateRecordSet() {
		Duration d = Duration.ofMillis(50L);
		consumerMock.subscribe(Arrays.asList("rob"));
		expect(consumerMock.poll(d)).andAnswer(() -> {
			assertEquals(service.getStatus().getState(), ExtensionState.RUNNING);
			return CRS(
					CR("foo", part1),
					CR("foo", part2),
					CR("bar", part1),
					CR("bob", part2),
					CR("pop", part1),
					CR("pop", part2)
				);
		});
		caelumMock.registerSymbol(toSet("foo", "bar", "bob", "pop"));
		expectLastCall().andAnswer(() -> { close.set(true); return null; });
		consumerMock.commitSync();
		consumerMock.close();
		expectLastCall().andAnswer(() -> {
			assertTrue(close.get());
			assertTrue(clear.get());
			assertEquals(service.getStatus().getState(), ExtensionState.DEAD);
			return null;
		});
		control.replay();
		
		service.run();
		
		control.verify();
	}
	
	@Test
	public void testRun_ShouldAvoidDuplicatesBetweenRecordSets() {
		Duration d = Duration.ofMillis(50);
		consumerMock.subscribe(Arrays.asList("rob"));
		expect(consumerMock.poll(d)).andAnswer(() -> {
			return CRS(
					CR("bak", part1),
					CR("bug", part2),
					CR("sol", part1)
				);
		});
		caelumMock.registerSymbol(toSet("bak", "bug", "sol"));
		consumerMock.commitSync();
		expect(consumerMock.poll(d)).andAnswer(() -> {
			return CRS(
					CR("bak", part1),
					CR("bob", part2),
					CR("sol", part1),
					CR("gas", part2)
				);
		});
		caelumMock.registerSymbol(toSet("bob", "gas"));
		consumerMock.commitSync();
		expect(consumerMock.poll(d)).andAnswer(() -> {
			close.set(true);
			return CRS(
					CR("bug", part2),
					CR("gas", part1)
				);
		});
		consumerMock.commitSync();
		consumerMock.close();
		control.replay();
		
		service.run();
		
		control.verify();
	}
	
	@Test
	public void testRun_ShouldClearCache() {
		Duration d = Duration.ofMillis(50);
		consumerMock.subscribe(Arrays.asList("rob"));
		expect(consumerMock.poll(d)).andAnswer(() -> {
			return CRS(
					CR("amp", part1),
					CR("ups", part2),
					CR("gap", part1)
				);
		});
		caelumMock.registerSymbol(toSet("amp", "ups", "gap"));
		consumerMock.commitSync();
		expect(consumerMock.poll(d)).andAnswer(() -> {
			clear.set(true);
			throw new WakeupException();
		});
		expect(consumerMock.poll(d)).andAnswer(() -> {
			assertFalse(clear.get());
			return CRS(
					CR("amp", part2),
					CR("ups", part1),
					CR("bob", part2)
				);
		});
		caelumMock.registerSymbol(toSet("amp", "ups", "bob"));
		consumerMock.commitSync();
		expect(consumerMock.poll(d)).andAnswer(() -> {
			close.set(true);
			throw new WakeupException();
		});
		consumerMock.close();
		control.replay();
		
		service.run();
		
		control.verify();
	}

}
