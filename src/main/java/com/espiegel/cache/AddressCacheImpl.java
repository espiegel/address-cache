package com.espiegel.cache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created by Eidan on 10/28/2015.
 *
 * This implementation of Address Cache maintains an internal set, stack and queue.
 * It also has an executor to schedule the removal of expired elements.
 */
public class AddressCacheImpl implements AddressCache {

	private static final int THREAD_POOL_SIZE = 2;
	private static final int EVICTION_DELAY_IN_MILLIS = 5000;
	private static final int QUEUE_INITIAL_CAPACITY = 10;

	private final Logger logger = LoggerFactory.getLogger(AddressCacheImpl.class);

	private final Stack<InetAddress> stack = new Stack<>();
	private final Set<InetAddress> set = Collections.synchronizedSet(new HashSet<>());
	private final Queue<AddressWithExpiration> queue = new PriorityBlockingQueue<>(QUEUE_INITIAL_CAPACITY, (a1, a2) -> Long.compare(a1.expiration, a2.expiration));
	private final ScheduledExecutorService executor = new ScheduledThreadPoolExecutor(THREAD_POOL_SIZE);
	private boolean isClosed = false;

	public AddressCacheImpl() {
		executor.scheduleAtFixedRate(() -> {
			AddressWithExpiration addressWithExpiration = queue.peek();
			while(addressWithExpiration != null && addressWithExpiration.expiration < System.currentTimeMillis()) {
				queue.poll();
				remove(addressWithExpiration.address);
				addressWithExpiration = queue.peek();
			}
		}, 0, EVICTION_DELAY_IN_MILLIS, TimeUnit.MILLISECONDS);
	}

	// O(lgn) time
	public boolean offerWithExpiration(InetAddress address, int expirationMillis) {
		checkNotClosed();

		long timestamp = System.currentTimeMillis() + expirationMillis;
		boolean success = queue.add(new AddressWithExpiration(address, timestamp));
		if(success) {
			offer(address);
		}
		logger.debug("inserted address {} with timestamp = {}", address, timestamp);
		return success;

	}

	// O(1) time
	public boolean offer(InetAddress address) {
		checkNotClosed();

		logger.debug("offering {}", address);
		if(!set.contains(address)) {
			stack.push(address);
			return set.add(address);
		} else {
			stack.push(address);
			return true;
		}
	}

	// O(1) time
	public boolean contains(InetAddress address) {
		checkNotClosed();

		return set.contains(address);
	}

	// O(1) time
	public boolean remove(InetAddress address) {
		checkNotClosed();

		logger.debug("removing {}", address);
		if(set.contains(address)) {
			set.remove(address);
			return true;
		} else {
			return false;
		}
	}

	// O(n) time
	public InetAddress peek() {
		checkNotClosed();

		while(!isEmpty()) {
			InetAddress address = stack.peek();
			if(set.contains(address)) {
				return address;
			} else {
				stack.pop();
			}
		}
		return null;
	}

	// O(n) time
	public InetAddress remove() {
		checkNotClosed();

		if(isEmpty()) {
			return null;
		} else {
			InetAddress address = stack.pop();
			if(set.contains(address)) {
				logger.debug("removing {}", address);
				set.remove(address);
				return address;
			} else {
				return remove();
			}
		}
	}

	// O(1) time if the cache is not empty. Otherwise this operation blocks until
	// an element is inserted.
	public InetAddress take() throws InterruptedException {
		while(true) {
			checkNotClosed();

			if(!isEmpty()) {
				InetAddress address = stack.pop();
				if(set.contains(address)) {
					logger.debug("taking {}", address);
					set.remove(address);
					return address;
				}
			}
		}
	}

	// O(n) time
	public void close() {
		if(!isClosed) {
			logger.debug("Closing...");
			set.clear();
			queue.clear();
			stack.clear();

			executor.shutdownNow();
			isClosed = true;
		}
	}

	// O(1) time
	public int size() {
		return set.size();
	}

	// O(1) time
	public boolean isEmpty() {
		return set.isEmpty();
	}

	private void checkNotClosed() {
		if(isClosed) {
			throw new IllegalStateException("AddressCache is closed!");
		}
	}

	private class AddressWithExpiration {
		InetAddress address;
		long expiration;

		public AddressWithExpiration(InetAddress address, long expiration) {
			this.address = address;
			this.expiration = expiration;
		}
	}
}
