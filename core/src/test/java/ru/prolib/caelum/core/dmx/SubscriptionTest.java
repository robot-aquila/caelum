package ru.prolib.caelum.core.dmx;

import static org.junit.Assert.*;
import static org.easymock.EasyMock.*;

import java.util.ConcurrentModificationException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.easymock.IMocksControl;
import org.junit.Before;
import org.junit.Test;

@SuppressWarnings("unchecked")
public class SubscriptionTest {
	IMocksControl control;
	Message<Integer, String> lastMsg;
	LinkedNode<Message<Integer, String>> lastMsgNode;
	MessageDispatcher<Integer, String> dispatcherMock;
	AtomicReference<LinkedNode<Message<Integer, String>>> last;
	AtomicBoolean closed;
	Subscription<Integer, String> service;
	LinkedNode<Message<Integer, String>> nodeMsgMock;
	
	@Before
	public void setUp() throws Exception {
		control = createStrictControl();
		nodeMsgMock = control.createMock(LinkedNode.class);
		dispatcherMock = control.createMock(MessageDispatcher.class);
		lastMsg = new Message<>();
		lastMsgNode = new LinkedNode<Message<Integer, String>>(lastMsg);
		last = new AtomicReference<>(lastMsgNode);
		closed = new AtomicBoolean();
		service = new Subscription<>(123, last, closed);
	}
	
	@Test
	public void testGetters() {
		assertEquals(Integer.valueOf(123), service.getSubscriber());
	}
	
	@Test
	public void testIsClosed() {
		assertFalse(service.isClosed());
		
		closed.set(true);
		
		assertTrue(service.isClosed());
	}
	
	@Test
	public void testClose() {
		service.close();
		
		assertTrue(closed.get());
	}
	
	@Test
	public void testNextOrNull_ShouldThrowIfClosed() {
		closed.set(true);
		
		assertThrows(IllegalStateException.class, () -> service.nextOrNull());
	}
	
	@Test
	public void testNextOrNull_ShouldReturnNullIfNoMessages() {
		assertNull(service.nextOrNull());
	}
	
	@Test
	public void testNextOrNull_ShouldThrowInCaseOfConcurrentModification() {
		last.set(nodeMsgMock);
		expect(nodeMsgMock.nextOrNull()).andAnswer(() -> {
			// any change should cause exception
			last.set(new LinkedNode<Message<Integer, String>>(new Message<>(null, "far")));
			return new LinkedNode<>(new Message<>(null, "foo"));
		});
		control.replay();
		
		assertThrows(ConcurrentModificationException.class, () -> service.nextOrNull());
	}
	
	@Test
	public void testNextOrNull_ShouldReturnBroadcastMessages() {
		Message<Integer, String> bcstMsg = new Message<>(null, "zoo"); 
		lastMsgNode.append(bcstMsg);
		
		Message<Integer, String> actual = service.nextOrNull();
		
		assertNotNull(actual);
		assertEquals(new Message<>(service, "zoo"), actual);
		assertSame(bcstMsg, last.get().getPayload());
	}
	
	@Test
	public void testNextOrNull_ShouldSkipPrivateMessagesForOthers() {
		ISubscription<Integer>
			subMock1 = control.createMock(ISubscription.class),
			subMock2 = control.createMock(ISubscription.class);
		Message<Integer, String>
			privMsg1 = new Message<>(subMock1, "gap"),
			privMsg2 = new Message<>(subMock2, "bob"),
			mineMsg = new Message<>(service, "dub");
		
		lastMsgNode.append(privMsg1);
		assertNull(service.nextOrNull());
		assertSame(privMsg1, last.get().getPayload());
		
		lastMsgNode.append(privMsg2);
		assertNull(service.nextOrNull());
		assertSame(privMsg2, last.get().getPayload());
		
		lastMsgNode.append(mineMsg);
		assertEquals(new Message<>(service, "dub"), service.nextOrNull());
		assertSame(mineMsg, last.get().getPayload());
	}
	
	@Test
	public void testNextOrNull_ShouldReturnPrivateMessages() {
		ISubscription<Integer> subMock1 = control.createMock(ISubscription.class);
		lastMsgNode.append(new Message<>(null, "map"));
		lastMsgNode.append(new Message<>(subMock1, "gap"));
		lastMsgNode.append(new Message<>(service, "dub"));
		
		assertEquals(new Message<>(service, "map"), service.nextOrNull());
		assertEquals(new Message<>(service, "dub"), service.nextOrNull());
		assertNull(service.nextOrNull());
	}
	
	@Test
	public void testNextOrNull_ShouldReturnCorrectSequenceEvenLastIsFarAway() {
		lastMsgNode.append(new Message<>(null, "zoo"));
		lastMsgNode.append(new Message<>(null, "bar"));
		lastMsgNode.append(new Message<>(null, "bob"));
		lastMsgNode.append(new Message<>(null, "gap"));
		lastMsgNode.append(new Message<>(null, "foo"));
		
		assertEquals(new Message<>(service, "zoo"), service.nextOrNull());
		assertEquals(new Message<>(service, "bar"), service.nextOrNull());
		assertEquals(new Message<>(service, "bob"), service.nextOrNull());
		assertEquals(new Message<>(service, "gap"), service.nextOrNull());
		assertEquals(new Message<>(service, "foo"), service.nextOrNull());
		assertNull(service.nextOrNull());
	}
	
	@Test
	public void testNextOrNull_EachSubscriptionHasItsOwnSequence() {
		Subscription<Integer, String> service1 = new Subscription<>(456, lastMsgNode);
		Subscription<Integer, String> service2 = new Subscription<>(456, lastMsgNode);

		lastMsgNode.append(new Message<>(null, "zoo"));
		lastMsgNode.append(new Message<>(service1, "bar"));
		lastMsgNode.append(new Message<>(null, "bob"));
		lastMsgNode.append(new Message<>(service2, "gap"));
		lastMsgNode.append(new Message<>(null, "foo"));
		
		assertEquals(new Message<>(service1, "zoo"), service1.nextOrNull());
		assertEquals(new Message<>(service1, "bar"), service1.nextOrNull());
		assertEquals(new Message<>(service1, "bob"), service1.nextOrNull());
		assertEquals(new Message<>(service1, "foo"), service1.nextOrNull());
		assertNull(service1.nextOrNull());
		
		assertEquals(new Message<>(service2, "zoo"), service2.nextOrNull());
		assertEquals(new Message<>(service2, "bob"), service2.nextOrNull());
		assertEquals(new Message<>(service2, "gap"), service2.nextOrNull());
		assertEquals(new Message<>(service2, "foo"), service2.nextOrNull());
		assertNull(service2.nextOrNull());
	}

}
