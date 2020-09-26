package ru.prolib.caelum.core.dmx;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.apache.commons.lang3.Range;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.BasicConfigurator;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageDispatcherIT {
	static final Logger logger = LoggerFactory.getLogger(MessageDispatcherIT.class);
	static final long NO_DELAY = 0;
	
	static class MyPlannedMessage {
		private final String payload;
		private final long delay;
		private final String recipientId;
		private final CountDownLatch countWhenReceived;
		
		public MyPlannedMessage(String payload, long delay, String recipientId, CountDownLatch countWhenReceived) {
			this.payload = payload;
			this.delay = delay;
			this.recipientId = recipientId;
			this.countWhenReceived = countWhenReceived;
		}
		
		/**
		 * Private message constructor.
		 * <p>
		 * @param payload - message payload
		 * @param delay - delay before sending message in ms. This delay means - global delay between messages.
		 * @param recipientId - recipient ID
		 */
		public MyPlannedMessage(String payload, long delay, String recipientId) {
			this(payload, delay, recipientId, null);
		}
		
		/**
		 * Broadcast message constructor.
		 * <p>
		 * @param payload - message payload
		 * @param delay - delay before sending message in ms.
		 */
		public MyPlannedMessage(String payload, long delay) {
			this(payload, delay, null, null);
		}
		
		public MyPlannedMessage(String payload, long delay, CountDownLatch countWhenReceived) {
			this(payload, delay, null, countWhenReceived);
		}
		
		public String getPayload() {
			return payload;
		}
		
		public long getDelay() {
			return delay;
		}
		
		public String getRecipientId() {
			return recipientId;
		}
		
		public void count() {
			if ( countWhenReceived != null ) {
				countWhenReceived.countDown();
			}
		}
		
		@Override
		public String toString() {
			return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
					.append("recipientId", recipientId)
					.append("delay", delay)
					.append("payload", payload)
					.build();
		}
		
	}
	
	static class MyReceivedMessage {
		private final String recipientId, threadId;
		private final MyPlannedMessage message;
		private final long time;
		
		public MyReceivedMessage(String recipientId, String threadId, MyPlannedMessage message, long time) {
			this.recipientId = recipientId;
			this.threadId = threadId;
			this.message = message;
			this.time = time;
		}
		
		public String getRecipientId() {
			return recipientId;
		}
		
		public String getThreadId() {
			return threadId;
		}
		
		public MyPlannedMessage getMessage() {
			return message;
		}
		
		public long getTime() {
			return time;
		}
		
		@Override
		public String toString() {
			return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
					.append("recipientId", recipientId)
					.append("threadId", threadId)
					.append("time", time)
					.append("message", message)
					.build();
		}
		
	}
	
	static class MyReceivedMessageComparator implements Comparator<Pair<MyReceivedMessage, Integer>> {

		@Override
		public int compare(Pair<MyReceivedMessage, Integer> a, Pair<MyReceivedMessage, Integer> b) {
			MyReceivedMessage am = a.getLeft(), bm = b.getLeft();
			Integer an = a.getRight(), bn = b.getRight();
			int r;
			if ( (r = Long.compare(am.time, bm.time)) != 0 ) return r;
			if ( (r = am.recipientId.compareTo(bm.recipientId)) != 0 ) return r;
			if ( (r = an.compareTo(bn)) != 0 ) return r;
			return r;
		}
		
	}
	
	static class MyRecipient {
		private final String id;
		private final AtomicLong startTime;
		private final Range<Long> delayRange;
		private final BlockingQueue<MyReceivedMessage> receivedMessages;
		
		public MyRecipient(String recipientId,
				AtomicLong startTime,
				Range<Long> delayRange,
				BlockingQueue<MyReceivedMessage> receivedMessages) {
			this.id = recipientId;
			this.startTime = startTime;
			this.delayRange = delayRange;
			this.receivedMessages = receivedMessages;
		}
		
		public MyRecipient(String recipientId, AtomicLong startTime, Range<Long> delayRange) {
			this(recipientId, startTime, delayRange, new LinkedBlockingQueue<>());
		}
		
		public MyRecipient(String recipientId, AtomicLong startTime) {
			this(recipientId, startTime, null);
		}
		
		public Collection<MyReceivedMessage> getReceivedMessages() {
			return receivedMessages;
		}
		
		public String getId() {
			return id;
		}
		
		public void receive(MyPlannedMessage msg) {
			if ( delayRange != null ) {
				long delay = ThreadLocalRandom.current().nextLong(delayRange.getMinimum(), delayRange.getMaximum());
				try {
					Thread.sleep(delay);
				} catch ( InterruptedException e ) {
					logger.error("Unexpected interruption: ", e);
				}
			}
			receivedMessages.add(new MyReceivedMessage(id, Thread.currentThread().getName(),
					msg, System.currentTimeMillis() - startTime.get()));
			msg.count();
		}
		
		@Override
		public String toString() {
			return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
				.append("id", id)
				.build();
		}
		
	}
	
	static class MyDeliveryThread implements Runnable {
		private final IMessageDispatcher<MyRecipient, MyPlannedMessage> dispatcher;
		private final CountDownLatch finished;
		
		public MyDeliveryThread(IMessageDispatcher<MyRecipient, MyPlannedMessage> dispatcher, CountDownLatch finished) {
			this.dispatcher = dispatcher;
			this.finished = finished;
		}

		@Override
		public void run() {
			Message<MyRecipient, MyPlannedMessage> msg;
			for ( ;; ) {
				try {
					msg = dispatcher.next();
					if ( msg == null ) {
						break;
					}
					msg.getSubscriber().receive(msg.getPayload());
					dispatcher.processed(msg);
				} catch ( Throwable t ) {
					logger.error("Unexpected exception: ", t);
				}
			}
			finished.countDown();
		}
		
	}

	public static List<MyPlannedMessage> generateBroadcastMessages(int num, String pfx, long maxDelay) {
		List<MyPlannedMessage> messages = new ArrayList<>();
		ThreadLocalRandom rnd = ThreadLocalRandom.current();
		for ( int i = 0; i < num; i ++ ) {
			long delay = maxDelay == NO_DELAY ? NO_DELAY : rnd.nextLong(0, maxDelay);
			messages.add(new MyPlannedMessage(pfx + String.format("%05d", i), delay));
		}
		return messages;
	}
	
	public static List<MyPlannedMessage> generateBroadcastMessages(int num, String pfx) {
		return generateBroadcastMessages(num, pfx, NO_DELAY);
	}
	
	public static List<MyRecipient> generateRecipients(int num, String idPfx, long maxDelay) {
		AtomicLong startTime = new AtomicLong(System.currentTimeMillis());
		ThreadLocalRandom rnd = ThreadLocalRandom.current();
		List<MyRecipient> recipients = new ArrayList<>();
		for ( int i = 0; i < num; i ++ ) {
			Range<Long> r = maxDelay > 0 ? Range.between(0L, rnd.nextLong(1, maxDelay)) : null;
			recipients.add(new MyRecipient(idPfx + i, startTime, r));
		}
		return recipients;
	}
	
	public static List<MyRecipient> generateRecipients(int num, String idPfx) {
		return generateRecipients(num, idPfx, NO_DELAY);
	}
	
	public static List<Pair<MyReceivedMessage, Integer>> toMsgWithIndexList(Collection<MyReceivedMessage> messages) {
		AtomicInteger n = new AtomicInteger();
		return messages.stream().map(m -> Pair.of(m, n.getAndIncrement())).collect(Collectors.toList());
	}
	
	public static void sortAndDumpReceivedMessages(List<MyRecipient> recipients) {
		List<Pair<MyReceivedMessage, Integer>> allMessages = new ArrayList<>();
		recipients.stream().forEach(r -> allMessages.addAll(toMsgWithIndexList(r.receivedMessages)));
		Collections.sort(allMessages, new MyReceivedMessageComparator());
		StringBuilder sb = new StringBuilder().append(System.lineSeparator());
		allMessages.forEach(p -> sb
				.append(" T+").append(String.format("%04d", p.getLeft().time))
				.append(" Trd:").append(p.getLeft().threadId)
				.append(" Rcp:").append(p.getLeft().recipientId)
				.append(" Msg:").append(p.getLeft().message.payload)
				.append(" Idx:").append(p.getRight())
				.append(System.lineSeparator()));
		logger.debug(sb.toString());
	}
	
	public static void assertReceivedMessages(List<MyPlannedMessage> expected, MyRecipient recipient) {
		List<MyReceivedMessage> received = new ArrayList<>();
		received.addAll(recipient.getReceivedMessages());
		for ( int i = 0; i < expected.size(); i ++ ) {
			MyPlannedMessage pm = expected.get(i);
			MyReceivedMessage rm = received.get(i);
			assertEquals("Recipient " + recipient +  " message mismatch at#" + i, pm, rm.getMessage());
		}
		assertEquals("Recipient " + recipient + " expected message number mismatch", expected.size(), received.size());
	}
	
	@BeforeClass
	public static void setUpBeforeClass() {
		BasicConfigurator.resetConfiguration();
		BasicConfigurator.configure();
	}
	
	MessageDispatcher<MyRecipient, MyPlannedMessage> dispatcher;
	
	@Before
	public void setUp() throws Exception {
		dispatcher = new MessageDispatcher<>();
	}
	
	public List<Thread> createDeliveryThreads(CountDownLatch numThreads) {
		List<Thread> list = new ArrayList<>();
		for ( long i = 0; i < numThreads.getCount(); i ++ ) {
			list.add(new Thread(new MyDeliveryThread(dispatcher, numThreads), "Delivery-" + i));
		}
		return list;
	}
	
	@Test
	public void testWorkflow_WithoutLatencies() throws Exception {
		CountDownLatch threadsFinished, lastMsgReceived;
		createDeliveryThreads(threadsFinished = new CountDownLatch(8)).stream().forEach(t -> t.start());
		List<MyRecipient> recipients = generateRecipients(4, "Client-", 0);
		lastMsgReceived = new CountDownLatch(recipients.size());
		List<MyPlannedMessage> messages = generateBroadcastMessages(20, "TEXT#");
		messages.add(new MyPlannedMessage("FIN", 0, lastMsgReceived));
		recipients.stream().forEach(r -> dispatcher.register(r));
		recipients.get(0).startTime.set(System.currentTimeMillis());
		
		messages.stream().forEach(m -> dispatcher.broadcast(m));
		
		assertTrue(lastMsgReceived.await(1, TimeUnit.SECONDS));
		dispatcher.close();
		assertTrue(threadsFinished.await(1, TimeUnit.SECONDS));
		recipients.stream().forEach(r -> assertReceivedMessages(messages, r));
		logger.debug("testWorkflow_WithoutLatencies");
		sortAndDumpReceivedMessages(recipients);
	}
	
	@Test
	public void testWorkflow_WithRecipientLatencies() throws Exception {
		CountDownLatch threadsFinished, lastMsgReceived;
		createDeliveryThreads(threadsFinished = new CountDownLatch(8)).stream().forEach(t -> t.start());
		List<MyRecipient> recipients = generateRecipients(4, "Client-", 100);
		lastMsgReceived = new CountDownLatch(recipients.size());
		List<MyPlannedMessage> messages = generateBroadcastMessages(20, "TEXT#");
		messages.add(new MyPlannedMessage("FIN", 0, lastMsgReceived));
		recipients.stream().forEach(r -> dispatcher.register(r));
		recipients.get(0).startTime.set(System.currentTimeMillis());
		
		messages.stream().forEach(m -> dispatcher.broadcast(m));
		
		assertTrue(lastMsgReceived.await(6, TimeUnit.SECONDS)); // 50 * 100 = 5000 max time
		dispatcher.close();
		assertTrue(threadsFinished.await(1, TimeUnit.SECONDS));
		recipients.stream().forEach(r -> assertReceivedMessages(messages, r));
		logger.debug("testWorkflow_WithRecipientLatencies");
		sortAndDumpReceivedMessages(recipients);
	}
	
	@Test
	public void testWorkflow_WithMessageLatencies() throws Exception {
		CountDownLatch threadsFinished, lastMsgReceived;
		createDeliveryThreads(threadsFinished = new CountDownLatch(10)).stream().forEach(t -> t.start());
		List<MyRecipient> recipients = generateRecipients(6, "Bobby-", 0);
		lastMsgReceived = new CountDownLatch(recipients.size());
		List<MyPlannedMessage> messages = generateBroadcastMessages(20, "TEXT#", 100);
		
		messages.add(new MyPlannedMessage("FIN", 0, lastMsgReceived));
		recipients.stream().forEach(r -> dispatcher.register(r));
		recipients.get(0).startTime.set(System.currentTimeMillis());
		
		messages.stream().forEach(m -> {
			if ( m.delay != NO_DELAY ) {
				try { Thread.sleep(m.delay); } catch ( InterruptedException e ) { }
			}
			dispatcher.broadcast(m);
		});
		
		assertTrue(lastMsgReceived.await(1, TimeUnit.SECONDS));
		dispatcher.close();
		assertTrue(threadsFinished.await(1, TimeUnit.SECONDS));
		recipients.stream().forEach(r -> assertReceivedMessages(messages, r));
		logger.debug("testWorkflow_WithMessageLatencies");
		sortAndDumpReceivedMessages(recipients);
	}
	
	@Test
	public void testWorkflow_WithRecipientAndMessageLatencies() throws Exception {
		CountDownLatch threadsFinished, lastMsgReceived;
		createDeliveryThreads(threadsFinished = new CountDownLatch(10)).stream().forEach(t -> t.start());
		List<MyRecipient> recipients = generateRecipients(6, "Maggy-", 100);
		lastMsgReceived = new CountDownLatch(recipients.size());
		List<MyPlannedMessage> messages = generateBroadcastMessages(20, "TEXT#", 100);
		messages.add(new MyPlannedMessage("FIN", 0, lastMsgReceived));
		recipients.stream().forEach(r -> dispatcher.register(r));
		recipients.get(0).startTime.set(System.currentTimeMillis());
		
		messages.stream().forEach(m -> {
			if ( m.delay != NO_DELAY ) {
				try { Thread.sleep(m.delay); } catch ( InterruptedException e ) { }
			}
			dispatcher.broadcast(m);
		});
		
		assertTrue(lastMsgReceived.await(3, TimeUnit.SECONDS));
		dispatcher.close();
		assertTrue(threadsFinished.await(1, TimeUnit.SECONDS));
		recipients.stream().forEach(r -> assertReceivedMessages(messages, r));
		logger.debug("testWorkflow_WithRecipientAndMessageLatencies");
		sortAndDumpReceivedMessages(recipients);
	}
	
	@Test
	public void testWorkflow_WithRecipientLatenciesAndPrivateMessages() throws Exception {
		CountDownLatch threadsFinished, lastMsgReceived;
		createDeliveryThreads(threadsFinished = new CountDownLatch(2)).stream().forEach(t -> t.start());
		List<MyRecipient> recipients = generateRecipients(4, "Box-", 50);
		Map<String, ISubscription<MyRecipient>> rmap =  recipients.stream()
				.collect(Collectors.toMap(r -> r.id, r -> dispatcher.register(r)));
		lastMsgReceived = new CountDownLatch(recipients.size());
		List<MyPlannedMessage> messages = generateBroadcastMessages(7, "TEXT#");
		messages.add(3, new MyPlannedMessage("PRIV4Box-1", 50, "Box-1"));
		messages.add(7, new MyPlannedMessage("PRIV#Box-2", 25, "Box-2"));
		messages.add(9, new MyPlannedMessage("PRIV#Box-3", 40, "Box-3"));
		messages.add(new MyPlannedMessage("FIN", 0, lastMsgReceived));
		recipients.get(0).startTime.set(System.currentTimeMillis());
		messages.stream().forEach(m -> dispatcher.send(m, rmap.get(m.recipientId)));
		
		assertTrue(lastMsgReceived.await(1, TimeUnit.SECONDS));
		dispatcher.close();
		assertTrue(threadsFinished.await(1, TimeUnit.SECONDS));
		
		recipients.stream().forEach(r -> assertReceivedMessages(messages.stream()
				.filter(m -> m.recipientId == null || m.recipientId.equals(r.id)).collect(Collectors.toList()), r));
		logger.debug("testWorkflow_WithRecipientLatenciesAndPrivateMessages");
		sortAndDumpReceivedMessages(recipients);
	}
	
	@Test
	public void testWorkflow_NumberOfThreadsGreaterThanNumberOfReceivers() throws Exception {
		CountDownLatch threadsFinished, lastMsgReceived;
		createDeliveryThreads(threadsFinished = new CountDownLatch(8)).stream().forEach(t -> t.start());
		List<MyRecipient> recipients = generateRecipients(2, "Box-", 25);
		recipients.stream().forEach(r -> dispatcher.register(r));
		lastMsgReceived = new CountDownLatch(recipients.size());
		List<MyPlannedMessage> messages = generateBroadcastMessages(100, "BAKA#");
		messages.add(new MyPlannedMessage("FINAL MESSAGE", 0, lastMsgReceived));
		recipients.get(0).startTime.set(System.currentTimeMillis());
		messages.stream().forEach(m -> dispatcher.broadcast(m));
		
		assertTrue(lastMsgReceived.await(3, TimeUnit.SECONDS));
		dispatcher.close();
		assertTrue(threadsFinished.await(1, TimeUnit.SECONDS));
		
		recipients.stream().forEach(r -> assertReceivedMessages(messages, r));
		logger.debug("testWorkflow_NumberOfThreadsGreaterThanNumberOfReceivers");
		sortAndDumpReceivedMessages(recipients);
	}
	
	@Test
	public void testWorkflow_NumberOfThreadsLessThanNumberOfReceivers() throws Exception {
		CountDownLatch threadsFinished, lastMsgReceived;
		createDeliveryThreads(threadsFinished = new CountDownLatch(2)).stream().forEach(t -> t.start());
		List<MyRecipient> recipients = generateRecipients(8, "Box-", 25);
		recipients.stream().forEach(r -> dispatcher.register(r));
		lastMsgReceived = new CountDownLatch(recipients.size());
		List<MyPlannedMessage> messages = generateBroadcastMessages(100, "DEKA#");
		messages.add(new MyPlannedMessage("FINAL MESSAGE", 0, lastMsgReceived));
		recipients.get(0).startTime.set(System.currentTimeMillis());
		messages.stream().forEach(m -> dispatcher.broadcast(m));
		
		assertTrue(lastMsgReceived.await(6, TimeUnit.SECONDS));
		dispatcher.close();
		assertTrue(threadsFinished.await(1, TimeUnit.SECONDS));
		
		recipients.stream().forEach(r -> assertReceivedMessages(messages, r));
		logger.debug("testWorkflow_NumberOfThreadsLessThanNumberOfReceivers");
		sortAndDumpReceivedMessages(recipients);
	}

}
