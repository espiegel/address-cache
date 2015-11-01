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
 * <p>
 * This implementation of AddressCache maintains an internal set, stack and queue in order to
 * achieve the following time complexity per operation:
 * <p>
 * offer(InetAddress address) - O(n)
 * contains(InetAddress address) - O(1)
 * remove(InetAddress address) - O(n)
 * peek() - O(n)
 * remove() - O(n)
 * take() - O(n)
 * close() - O(n)
 * size() - O(1)
 * isEmpty() - O(1)
 */
public class AddressCacheImpl implements AddressCache {

    public static final int DEFAULT_EXPIRATION_TIME_IN_MILLIS = 10 * 1000;
    private static final int THREAD_POOL_SIZE = 2;
    private static final int EVICTION_DELAY_IN_MILLIS = 5 * 1000;
    private static final int QUEUE_INITIAL_CAPACITY = 10;

    private static final Object WRITE_LOCK = new Object();

    private final Logger logger = LoggerFactory.getLogger(AddressCacheImpl.class);

    private final Stack<InetAddress> stack = new Stack<>();
    private final Set<InetAddress> set = Collections.synchronizedSet(new HashSet<>());
    private final Queue<AddressWithExpiration> queue = new PriorityBlockingQueue<>(QUEUE_INITIAL_CAPACITY, (a1, a2) -> Long.compare(a1.expiration, a2.expiration));
    private final ScheduledExecutorService executor = new ScheduledThreadPoolExecutor(THREAD_POOL_SIZE);
    private boolean isClosed = false;

    public AddressCacheImpl() {
        executor.scheduleAtFixedRate(() -> {
            synchronized(WRITE_LOCK) {
                AddressWithExpiration addressWithExpiration = queue.peek();
                while(addressWithExpiration != null && addressWithExpiration.expiration < System.currentTimeMillis()) {
                    logger.info("evicting {} with timestamp {}", queue.peek().address, queue.peek().expiration);
                    queue.poll();
                    set.remove(addressWithExpiration.address);
                    stack.remove(addressWithExpiration.address);
                    addressWithExpiration = queue.peek();
                }
            }
        }, 0, EVICTION_DELAY_IN_MILLIS, TimeUnit.MILLISECONDS);
    }

    // O(n) time
    public boolean offer(InetAddress address) {
        synchronized(WRITE_LOCK) {
            checkNotClosed();

            if(set.contains(address)) {
                stack.remove(address);
                removeAddressFromQueue(address);
            }

            stack.push(address);
            set.add(address);
            long timestamp = System.currentTimeMillis() + DEFAULT_EXPIRATION_TIME_IN_MILLIS;
            queue.add(new AddressWithExpiration(address, timestamp));
            logger.debug("offering {} with timestamp {}", address, timestamp);
            return true;
        }
    }

    // O(1) time
    public boolean contains(InetAddress address) {
        checkNotClosed();

        return set.contains(address);
    }

    // O(n) time
    public boolean remove(InetAddress address) {
        synchronized(WRITE_LOCK) {
            checkNotClosed();

            if(set.contains(address)) {
                logger.debug("removing {}", address);
                set.remove(address);
                stack.remove(address);
                removeAddressFromQueue(address);
                return true;
            } else {
                return false;
            }
        }
    }

    // O(1) time
    public InetAddress peek() {
        checkNotClosed();

        if(!isEmpty()) {
            return stack.peek();
        } else {
            return null;
        }
    }

    // O(n) time
    public InetAddress remove() {
        synchronized(WRITE_LOCK) {
            checkNotClosed();

            if(!isEmpty()) {
                InetAddress address = stack.pop();
                logger.debug("removing {}", address);
                set.remove(address);
                removeAddressFromQueue(address);
                return address;
            } else {
                return null;
            }
        }
    }

    // O(n) time if the cache is not empty. Otherwise this operation blocks until
    // an element is inserted.
    public InetAddress take() throws InterruptedException {
        while(true) {
            checkNotClosed();

            synchronized(WRITE_LOCK) {
                if(!isEmpty()) {
                    InetAddress address = stack.pop();
                    logger.debug("taking {}", address);
                    set.remove(address);
                    removeAddressFromQueue(address);
                    return address;
                }
            }
        }
    }

    // O(n) time
    public void close() {
        if(!isClosed) {
            synchronized(WRITE_LOCK) {
                logger.debug("Closing...");
                set.clear();
                queue.clear();
                stack.clear();

                executor.shutdownNow();
                isClosed = true;
            }
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

    private void removeAddressFromQueue(InetAddress address) {
        queue.removeIf(a -> a.address.equals(address));
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
