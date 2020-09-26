package ru.prolib.caelum.core.dmx;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;

public class LinkedNodeTest {

	@Before
	public void setUp() throws Exception {
	}

	@Test
	public void testGetDistanceToLast() {
		LinkedNode.Distance<String> d = null;
		LinkedNode<String> root = new LinkedNode<>("foo");
		
		d = root.getDistanceToLast(root);
		assertEquals(0, d.getDistance());
		assertSame(root, d.getNode());
		
		LinkedNode<String> node1 = root.append("bar");
		d = root.getDistanceToLast(root);
		assertEquals(1, d.getDistance());
		assertSame(node1, d.getNode());
		
		LinkedNode<String> node2 = node1.append("zoo");
		d = root.getDistanceToLast(root);
		assertEquals(2, d.getDistance());
		assertSame(node2, d.getNode());
	}
	
	@Test
	public void testGetDistanceToLast_ShouldWorkForAnyNodes() {
		LinkedNode.Distance<String> d = null;
		LinkedNode<String>
				root = new LinkedNode<>("foo"),
				node1 = root.append("bar"),
				node2 = root.append("zoo");
		
		for ( LinkedNode<String> basis : Arrays.asList(root, node1, node2) ) {
			d = basis.getDistanceToLast(root);
			assertEquals(2, d.getDistance());
			assertSame(node2, d.getNode());
			
			d = basis.getDistanceToLast(node1);
			assertEquals(1, d.getDistance());
			assertSame(node2, d.getNode());
			
			d = basis.getDistanceToLast(node2);
			assertEquals(0, d.getDistance());
			assertSame(node2, d.getNode());
		}
	}
	
	@Test
	public void testNextOrNull() {
		LinkedNode<String> root = new LinkedNode<>("foo");
		
		assertNull(root.nextOrNull());
		
		LinkedNode<String> next = root.append("bar");
		
		assertSame(next, root.nextOrNull());
	}
	
	@Test
	public void testNextOrWait() throws Exception {
		LinkedNode<String> root = new LinkedNode<>("buz");
		int count = 3;
		CountDownLatch started = new CountDownLatch(count);
		@SuppressWarnings("unchecked")
		CompletableFuture<LinkedNode<String>> r[] = new CompletableFuture[count];
		for ( int i = 0; i < count; i ++ ) {
			final CompletableFuture<LinkedNode<String>> f = r[i] = new CompletableFuture<>();
			new Thread(() -> {
				started.countDown();
				try {
					LinkedNode<String> n = root.nextOrWait();
					f.complete(n);
				} catch ( InterruptedException e ) { }
			}).start();
		}
		
		assertTrue(started.await(1, TimeUnit.SECONDS));
		root.append("pop");
		CompletableFuture.allOf(r).get(1, TimeUnit.SECONDS);
		for ( int i = 1; i < count; i ++ ) {
			assertSame(r[0].get(), r[i].get());
		}
		assertEquals("pop", r[0].get().getPayload());
	}
	
	@Test
	public void testGetLast() {
		LinkedNode<String> root = new LinkedNode<>("bob");
		
		assertSame(root, root.getLast());
		
		LinkedNode<String> node1 = root.append("buy");
		LinkedNode<String> node2 = root.append("bay");
		LinkedNode<String> node3 = root.append("top");
		
		assertSame(node3, root.getLast());
		assertSame(node3, node1.getLast());
		assertSame(node3, node2.getLast());
		assertSame(node3, node3.getLast());
		
		root.append("hello");
		root.append("world");
		LinkedNode<String> node6 = root.append("good bye");
		assertSame(node6, root.getLast());
		assertSame(node6, node1.getLast());
		assertSame(node6, node2.getLast());
		assertSame(node6, node3.getLast());
		assertSame(node6, node6.getLast());
	}

}
