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
 */
public class AddressCacheImpl implements AddressCache {

    private static final int THREAD_POOL_SIZE = 2;
    private static final int EVICTION_DELAY_IN_MILLIS = 5000;
    private static final int QUEUE_INITIAL_CAPACITY = 10;

    private final Logger logger = LoggerFactory.getLogger(AddressCacheImpl.class);

    private Stack<InetAddress> stack = new Stack<>();
    private Set<InetAddress> set = Collections.synchronizedSet(new HashSet<>());
    private Queue<AddressWithExpiration> queue = new PriorityBlockingQueue<>(QUEUE_INITIAL_CAPACITY, (a1, a2) -> Long.compare(a1.expiration, a2.expiration));
    private ScheduledExecutorService executor = new ScheduledThreadPoolExecutor(THREAD_POOL_SIZE);
    private boolean isClosed = false;

    public AddressCacheImpl() {
        executor.scheduleAtFixedRate(() -> {
            while(true) {
                AddressWithExpiration addressWithExpiration = queue.peek();

                if(addressWithExpiration != null) {
                    logger.info("Found expiration = {}, current time = {}", addressWithExpiration.expiration, System.currentTimeMillis());
                    if(addressWithExpiration.expiration < System.currentTimeMillis()) {
                        queue.poll();
                        remove(addressWithExpiration.address);
                    } else {
                        break;
                    }
                }
            }
        }, 0, EVICTION_DELAY_IN_MILLIS, TimeUnit.MILLISECONDS);
    }

    public boolean offerWithExpiration(InetAddress address, int expirationMillis) {
        checkNotClosed();

        long timestamp = System.currentTimeMillis() + expirationMillis;
        boolean success = queue.add(new AddressWithExpiration(address, timestamp));
        if(success) {
            offer(address);
        }
        logger.info("inserted address {} with timestamp = {}", address, timestamp);
        return success;

    }

    public boolean offer(InetAddress address) {
        checkNotClosed();

        logger.info("offering {}", address);
        if(!set.contains(address)) {
            stack.push(address);
            return set.add(address);
        } else {
            stack.removeElement(address);
            stack.push(address);
            return true;
        }
    }

    public boolean contains(InetAddress address) {
        return set.contains(address);
    }

    public boolean remove(InetAddress address) {
        checkNotClosed();

        logger.info("removing {}", address);
        if(set.contains(address)) {
            set.remove(address);
            stack.remove(address);
            return true;
        } else {
            return false;
        }
    }

    public InetAddress peek() {
        return isEmpty() ? null : stack.peek();
    }

    public InetAddress remove() {
        if(isEmpty()) {
            return null;
        } else {
            InetAddress address = stack.pop();
            logger.info("removing {}", address);
            set.remove(address);
            return address;
        }
    }

    public InetAddress take() throws InterruptedException {
        while(true) {
            checkNotClosed();

            if(!isEmpty()) {
                InetAddress address = stack.pop();
                logger.info("taking {}", address);
                set.remove(address);
                return address;
            }
        }
    }

    private void checkNotClosed() {
        if(isClosed) {
            throw new IllegalStateException("AddressCache is closed!");
        }
    }

    public void close() {
        if(!isClosed) {
            set.clear();
            stack.clear();
            queue.clear();
            executor.shutdownNow();
            isClosed = true;
        }
    }

    public int size() {
        return set.size();
    }

    public boolean isEmpty() {
        return set.isEmpty();
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
