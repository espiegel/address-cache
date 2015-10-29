package com.espiegel.cache;

/**
 * Created by Eidan on 10/28/2015.
 */

import java.net.InetAddress;

/**
 * This is an interface for a fictional {@link InetAddress} cache. The cache
 * maintains a "Last-In-First-Out" (LIFO) retrieve policy and a "First-In-
 * First-Out" (FIFO) eviction policy.
 * <p>
 * Methods such as {@link #peek()}, {@link #remove()} and {@link #take()}
 * retrieve the most recently added element and an internal cleanup task that
 * runs in periodic intervals removes the oldest elements from the cache.
 * <p>
 * The cleanup task must run every 5 seconds and the caching time of an element
 * must be configurable by the user.
 * <p>
 * The goal is to write the most beautiful and functionally correct code possible
 * (including proof of correctness - i.e. tests).
 * <p>
 * Please mention the asymptotic complexity (Big-Oh) for each method of your
 * implementation.
 * <p>
 * offer() - Adds an element and returns true on success. An element can
 * exist only once in the cache and consecutive offers move the element to
 * the front of the list.
 * <p>
 * contains() - Returns true if an element exists in the cache
 * <p>
 * remove(InetAddress) - Removes the given address from the cache
 * <p>
 * peek() - Returns the most recently added element or null if the cache
 * is empty
 * <p>
 * remove() - Retrieves and removes the most recently added element or
 * null of the cache is empty
 * <p>
 * take() - Retrieves and removes the most recently added element, waiting
 * if necessary until an element becomes available
 * <p>
 * size() - Returns the size of the cache
 * <p>
 * isEmpty() - Returns true if the cache is empty
 * <p>
 * close() - Closes the cache and releases all resources
 */
public interface AddressCache {

    /**
     * Adds the given {@link InetAddress} and will evict it within expirationMillis milliseconds. Returns {@code true} on success.
     */
    public boolean offerWithExpiration(InetAddress address, int expirationMillis);

    /**
     * Adds the given {@link InetAddress} and returns {@code true} on success.
     */
    public boolean offer(InetAddress address);

    /**
     * Returns {@code true} if the given {@link InetAddress}
     * is in the {@link AddressCache}.
     */
    public boolean contains(InetAddress address);

    /**
     * Removes the given {@link InetAddress} and returns {@code true}
     * on success.
     */
    public boolean remove(InetAddress address);

    /**
     * Returns the most recently added {@link InetAddress} and returns
     * {@code null} if the {@link AddressCache} is empty.
     */
    public InetAddress peek();

    /**
     * Removes and returns the most recently added {@link InetAddress} and
     * returns {@code null} if the {@link AddressCache} is empty.
     */
    public InetAddress remove();

    /**
     * Retrieves and removes the most recently added {@link InetAddress},
     * waiting if necessary until an element becomes available.
     */
    public InetAddress take() throws InterruptedException;

    /**
     * Closes the {@link AddressCache} and releases all resources.
     */
    public void close();

    /**
     * Returns the number of elements in the {@link AddressCache}.
     */
    public int size();

    /**
     * Returns {@code true} if the {@link AddressCache} is empty.
     */
    public boolean isEmpty();
}
