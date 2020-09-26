package ru.prolib.caelum.core.dmx;

import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BinaryOperator;

public class LinkedNode<Payload> {
	
	/**
	 * Describes distance to specific node.
	 */
	static class Distance<Payload> implements Comparable<Distance<Payload>> {
		private final long distance;
		private final LinkedNode<Payload> node;
		
		public Distance(long distance, LinkedNode<Payload> node) {
			this.distance = distance;
			this.node = node;
		}
		
		public long getDistance() {
			return distance;
		}
		
		public LinkedNode<Payload> getNode() {
			return node;
		}
		
		@Override
		public int compareTo(Distance<Payload> other) {
			return Long.compareUnsigned(this.distance, other.distance);
		}
		
	}
	
	static class MaxDistance<Payload> implements BinaryOperator<Distance<Payload>> {

		@Override
		public Distance<Payload> apply(Distance<Payload> prev, Distance<Payload> x) {
			return prev == null || x.compareTo(prev) >= 0 ? x : prev;
		}
		
	}
	
	/**
	 * Node custom payload.
	 */
	private final Payload payload;
	
	/**
	 * The next node. Initially is null. Will be set just once.
	 */
	private final AtomicReference<LinkedNode<Payload>> next;
	
	/**
	 * The last known node. Initially is null. Can be set during scans. Moves forward only.
	 */
	private final AtomicReference<Distance<Payload>> last;
	
	private final Lock lock;
	private final Condition signal;
	private final MaxDistance<Payload> maxDistOperator = new MaxDistance<>();
	
	public LinkedNode(Payload payload,
			Lock lock,
			Condition signal,
			AtomicReference<LinkedNode<Payload>> next,
			AtomicReference<Distance<Payload>> last)
	{
		this.payload = payload;
		this.lock = lock;
		this.signal = signal;
		this.next = next;
		this.last = last;
	}
	
	public LinkedNode(Payload payload,
			Lock lock,
			AtomicReference<LinkedNode<Payload>> next,
			AtomicReference<Distance<Payload>> last)
	{
		this(payload, lock, lock.newCondition(), next, last);
	}
	
	public LinkedNode(Payload payload) {
		this(payload, new ReentrantLock(), new AtomicReference<>(), new AtomicReference<>());
	}
	
	public Payload getPayload() {
		return payload;
	}
	
	/**
	 * Get node next after this one or null if next node not defined.
	 * <p>
	 * @return node instance or null
	 */
	public LinkedNode<Payload> nextOrNull() {
		return next.get();
	}
	
	/**
	 * Get node next after this one or wait until next node defined then return it.
	 * <p>
	 * @return next node instance
	 * @throws InterruptedException - blocking method unexpectedly interrupted
	 */
	public LinkedNode<Payload> nextOrWait() throws InterruptedException {
		LinkedNode<Payload> x = next.get();
		if ( x != null ) return x;
		lock.lock();
		try {
			while ( x == null ) {
				if ( (x = next.get()) == null ) signal.await();
			}
		} finally {
			lock.unlock();
		}
		return x;
	}
	
	/**
	 * Find a last known node and determine distance to it starting from specifiec node.
	 * <p>
	 * This is service method. Not uses any class variables. Is here just because of type of payload.
	 * <p>
	 * @param node - starting node
	 * @return last node and distance to it
	 */
	public Distance<Payload> getDistanceToLast(LinkedNode<Payload> node) {
		LinkedNode<Payload> next = null;
		for ( int distance = 0; ; distance ++ ) {
			next = node.nextOrNull();
			if ( next == null ) {
				return new Distance<>(distance, node);
			}
			node = next;
		}
	}
	
	/**
	 * Get node currently last known.
	 * <p>
	 * A last node is needed to speed up operations.
	 * You do not need to track the tail.
	 * This call will refresh cached values if needed.
	 * <p>
	 * @return last known node
	 */
	public LinkedNode<Payload> getLast() {
		Distance<Payload> currDist = last.get();
		if ( currDist == null ) {
			currDist = getDistanceToLast(this);
			return last.accumulateAndGet(currDist, maxDistOperator).getNode();
		}
		LinkedNode<Payload> currLastNode = currDist.getNode();
		if ( currLastNode.nextOrNull() == null ) {
			return currLastNode;
		}
		// There is something new at the tail.
		// We should actualize the last known node.
		Distance<Payload> newDist = getDistanceToLast(currLastNode);
		newDist = new Distance<>(newDist.distance + currDist.distance, newDist.node);
		return last.accumulateAndGet(newDist, maxDistOperator).getNode();
	}
	
	/**
	 * Append the next node or return node what was already appended.
	 * <p>
	 * This call will wake up all waiting threads in case of next node append. 
	 * <p>
	 * @param node - node to append as next of this node
	 * @return null if argument was append as next node for this node or instance
	 * of next node which was already set prior this call
	 */
	private LinkedNode<Payload> appendOrNext(LinkedNode<Payload> node) {
		if ( next.compareAndSet(null, node) == false ) return next.get();
		lock.lock();
		try {
			signal.signalAll();
		} finally {
			lock.unlock();
		}
		return null;
	}
	
	/**
	 * Append a new node to the chain.
	 * <p>
	 * We need method with such signature to use in atomic operations.
	 * <p>
	 * @param node - node to append
	 * @return a node that possible the last node in the chain. The possible means that
	 * other nodes may be added during small amount of time between adding and returning
	 * the result (and of course during analyzing the result as well that will be outside).
	 * But that shouldn't be a problem because any node can be safely used to append or
	 * obtain nodes. Nodes always append to the end.
	 */
	public LinkedNode<Payload> append(LinkedNode<Payload> node) {
		// Go ahead and find a first node without next
		LinkedNode<Payload> next = null;
		LinkedNode<Payload> last = getLast();
		for ( ;; ) {
			next = last.appendOrNext(node);
			if ( next == null ) {
				// TODO: update last node?
				return node;
			}
			last = next;
		}
	}
	
	/**
	 * Append new node to the chain.
	 * <p>
	 * @param payload - payload of node to append
	 * @return last node in the chain. See {@link #append(LinkedNode)} for details.
	 */
	public LinkedNode<Payload> append(Payload payload) {
		return append(new LinkedNode<>(payload));
	}

}
