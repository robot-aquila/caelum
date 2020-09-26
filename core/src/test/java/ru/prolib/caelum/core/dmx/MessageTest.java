package ru.prolib.caelum.core.dmx;

import static org.junit.Assert.*;

import static org.easymock.EasyMock.*;

import org.easymock.IMocksControl;
import org.junit.Before;
import org.junit.Test;

public class MessageTest {
	IMocksControl control;
	ISubscription<Integer> subscription1;
	Message<Integer, String> firstMsg;
	LinkedNode<Message<Integer, String>> firstMsgNode;
	Message<Integer, String> service;

	@Before
	public void setUp() throws Exception {
		control = createStrictControl();
		firstMsg = new Message<>();
		firstMsgNode = new LinkedNode<>(firstMsg);
		subscription1 = new Subscription<>(567, firstMsgNode);
		service = new Message<>(subscription1, "bambr");
	}
	
	@Test
	public void testCtor2() {
		assertSame(subscription1, service.getSubscription());
		assertEquals(Integer.valueOf(567), service.getSubscriber());
		assertEquals("bambr", service.getPayload());
	}
	
	@Test
	public void testCtor0_InitialMessage() {
		service = new Message<>();
		assertNull(service.getSubscription());
		assertNull(service.getSubscriber());
		assertNull(service.getPayload());
	}
	
	@Test
	public void testToString() {
		assertEquals("Message[subscriber=567,payload=bambr]", service.toString());
	}
	
	@SuppressWarnings("unlikely-arg-type")
	@Test
	public void testEquals() {
		assertTrue(service.equals(service));
		assertTrue(service.equals(new Message<>(subscription1, "bambr")));
		assertFalse(service.equals(null));
		assertFalse(service.equals(this));
		assertFalse(service.equals(new Message<>(new Subscription<>(567, firstMsgNode), "bambr")));
		assertFalse(service.equals(new Message<>(new Subscription<>(123, firstMsgNode), "bambr")));
		assertFalse(service.equals(new Message<>(subscription1, "lord")));
	}

}
