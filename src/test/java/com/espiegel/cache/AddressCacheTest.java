package com.espiegel.cache;

import com.espiegel.cache.util.Utils;
import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.IntStream;

import static org.junit.Assert.*;

/**
 * Created by Eidan on 10/28/2015.
 */
public class AddressCacheTest {

    public static final int THREE_SECONDS = 3000;
    public static final int SIX_SECONDS = 6000;

    // Unit tests for all methods of the AddressCache interface
    @Test
    public void testAddressCacheOffer() {
        AddressCache addressCache = new AddressCacheImpl();

        boolean success = addressCache.offer(InetAddress.getLoopbackAddress());

        assertTrue(success);
        assertEquals(1, addressCache.size());
        assertFalse(addressCache.isEmpty());
    }

    @Test
    public void testAddressCacheOfferWithEviction() throws UnknownHostException, InterruptedException {
        AddressCache addressCache = new AddressCacheImpl();

        boolean success = addressCache.offerWithExpiration(InetAddress.getByName("www.google.com"), THREE_SECONDS);

        assertTrue(success);
        assertEquals(1, addressCache.size());
        assertFalse(addressCache.isEmpty());

        Thread.sleep(SIX_SECONDS);

        assertEquals(0, addressCache.size());
        assertTrue(addressCache.isEmpty());
    }

    @Test
    public void testAddressCacheContains() throws UnknownHostException {
        AddressCache addressCache = createAddressCacheWithEviction(true);

        assertTrue(addressCache.contains(InetAddress.getByName("www.google.com")));
        assertFalse(addressCache.contains(InetAddress.getByName("www.yahoo.com")));
    }

    @Test
    public void testAddressCacheRemoveSpecific() {
        AddressCache addressCache = new AddressCacheImpl();
        addressCache.offer(InetAddress.getLoopbackAddress());

        addressCache.remove(InetAddress.getLoopbackAddress());

        assertEquals(0, addressCache.size());
        assertTrue(addressCache.isEmpty());
    }

    @Test
    public void testPeek() throws UnknownHostException, InterruptedException {
        AddressCache addressCache = createAddressCacheWithEviction(false);

        assertEquals(InetAddress.getByName("www.github.com"), addressCache.peek());

        Thread.sleep(SIX_SECONDS);
        assertEquals(InetAddress.getByName("www.handy.com"), addressCache.peek());

        Thread.sleep(SIX_SECONDS);
        assertEquals(null, addressCache.peek());
    }

    @Test
    public void testAddressCacheRemoveFirst() {
        AddressCache addressCache = new AddressCacheImpl();
        addressCache.offer(InetAddress.getLoopbackAddress());

        InetAddress removed = addressCache.remove();
        assertEquals(removed, InetAddress.getLoopbackAddress());

        assertEquals(0, addressCache.size());
        assertTrue(addressCache.isEmpty());
    }

    @Test
    public void testTake() throws UnknownHostException, InterruptedException {
        AddressCache addressCache = createAddressCache();

        InetAddress address = addressCache.take();
        assertEquals(InetAddress.getByName("www.twitter.com"), address);

        address = addressCache.take();
        assertEquals(InetAddress.getByName("www.reddit.com"), address);

        while(!addressCache.isEmpty()) {
            addressCache.remove();
        }

        new Thread(() -> {
            try {
                Thread.sleep(THREE_SECONDS);
                addressCache.offer(InetAddress.getByName("9gag.com"));
            } catch(UnknownHostException | InterruptedException e) {
                e.printStackTrace();
                fail(e.getMessage());
            }
        }).start();

        address = addressCache.take();
        assertEquals(InetAddress.getByName("9gag.com"), address);
    }

    @Test(expected = IllegalStateException.class)
    public void testAddressCacheClose() throws UnknownHostException, InterruptedException {
        AddressCache addressCache = createAddressCacheWithEviction(true);

        addressCache.close();
        assertEquals(0, addressCache.size());
        assertTrue(addressCache.isEmpty());

        Thread.sleep(SIX_SECONDS);

        assertEquals(0, addressCache.size());
        assertTrue(addressCache.isEmpty());

        addressCache.offer(InetAddress.getByName("www.google.com"));
    }

    @Test
    public void testAddressCacheBeginsEmpty() {
        AddressCache addressCache = new AddressCacheImpl();
        assertEquals(0, addressCache.size());
        assertTrue(addressCache.isEmpty());
    }

    // Integration tests for a few scenarios
    @Test
    public void testAddressCacheCloseWhileWaitingForTake() throws UnknownHostException, InterruptedException {
        for(int i = 0; i < 5; i++) {
            AddressCache addressCache = new AddressCacheImpl();

            Thread t = new Thread(() -> {
                try {
                    addressCache.take();
                    fail("take operation was supposed to throw an IllegalStateException");
                } catch(IllegalStateException e) {
                    // Its supposed to throw this exception
                } catch(InterruptedException e) {
                    // This exception isn't supposed to be thrown
                    fail(e.getMessage());
                }
            });
            Thread t2 = new Thread(addressCache::close);

            t.start();
            t2.start();

            Thread.sleep(1000);

            assertTrue(addressCache.isEmpty());
        }
    }

    @Test
    public void testAddressCacheMultipleEviction() throws UnknownHostException, InterruptedException {
        AddressCache addressCache = createAddressCacheWithEviction(true);

        List<String> addresses = Arrays.asList("www.google.com", "www.handy.com", "www.github.com");
        for(int i = 0; i < addresses.size(); i++) {
            boolean success = addressCache.offerWithExpiration(InetAddress.getByName(addresses.get(i)), THREE_SECONDS * (i + 1));
            assertTrue(success);
        }

        assertEquals(addresses.size(), addressCache.size());
        assertFalse(addressCache.isEmpty());

        Thread.sleep(SIX_SECONDS);
        assertEquals(2, addressCache.size());

        Thread.sleep(SIX_SECONDS);
        assertEquals(0, addressCache.size());
        assertTrue(addressCache.isEmpty());
    }

    @Test
    public void testConsecutiveOffer() throws UnknownHostException {
        AddressCache addressCache = createAddressCache();

        assertEquals(InetAddress.getByName("www.twitter.com"), addressCache.peek());

        addressCache.offer(InetAddress.getByName("www.handy.com"));
        assertEquals(InetAddress.getByName("www.handy.com"), addressCache.peek());

        addressCache.offer(InetAddress.getByName("www.google.com"));
        assertEquals(InetAddress.getByName("www.google.com"), addressCache.peek());

        addressCache.offer(InetAddress.getByName("www.reddit.com"));
        assertEquals(InetAddress.getByName("www.reddit.com"), addressCache.peek());

        addressCache.offer(InetAddress.getByName("www.handy.com"));
        assertEquals(InetAddress.getByName("www.handy.com"), addressCache.peek());
    }

    @Test
    public void testConcurrency() throws UnknownHostException, InterruptedException {
        AddressCache addressCache = createAddressCache();

        List<String> addresses = Arrays.asList("www.cnn.com", "www.nbc.com", "www.yahoo.com", "www.espn.com", "www.wikipedia.org");
        List<Thread> threads = new ArrayList<>();

        addresses.forEach(a -> {
            threads.add(new Thread(Utils.unchecked(() -> addressCache.offerWithExpiration(InetAddress.getByName(a), THREE_SECONDS))));
        });

        threads.add(new Thread(Utils.unchecked(() -> addressCache.remove(InetAddress.getByName("www.google.com")))));
        threads.add(new Thread(Utils.unchecked(() -> addressCache.remove(InetAddress.getByName("www.github.com")))));
        threads.add(new Thread(Utils.unchecked(() -> addressCache.remove(InetAddress.getByName("www.reddit.com")))));

        threads.forEach(Thread::start);

        Thread.sleep(1000);

        assertFalse(addressCache.isEmpty());
        assertEquals(7, addressCache.size());

        Thread.sleep(10000);

        assertFalse(addressCache.isEmpty());
        assertEquals(2, addressCache.size());
    }

    @Test
    public void testConcurrency2() throws UnknownHostException, InterruptedException {
        AddressCache addressCache = createAddressCache();

        List<Thread> threads = new ArrayList<>();
        List<String> addresses = Arrays.asList("www.cnn.com", "www.nbc.com", "www.yahoo.com", "www.espn.com", "www.wikipedia.org");

        Random random = new Random();
        IntStream.range(0, 100).forEach(i -> {
            try {
                InetAddress address = InetAddress.getByName(addresses.get(random.nextInt(addresses.size())));
                threads.add(new Thread(() -> addressCache.offerWithExpiration(address, random.nextInt(10000))));
            } catch(UnknownHostException e) {
                e.printStackTrace();
                fail();
            }
        });

        threads.forEach(Thread::start);

        while(!addressCache.isEmpty()) {
            addressCache.remove();
        }

        Thread.sleep(15000);

        assertTrue(addressCache.isEmpty());
        assertEquals(0, addressCache.size());
    }

    private AddressCache createAddressCache() throws UnknownHostException {
        AddressCache addressCache = new AddressCacheImpl();
        List<String> addresses = Arrays.asList("www.google.com", "www.handy.com", "www.github.com", "www.reddit.com", "www.twitter.com");
        for(String address : addresses) {
            boolean success = addressCache.offer(InetAddress.getByName(address));
            assertTrue(success);
        }
        return addressCache;
    }

    private AddressCache createAddressCacheWithEviction(boolean isEvictionAscending) throws UnknownHostException {
        AddressCache addressCache = new AddressCacheImpl();
        List<String> addresses = Arrays.asList("www.google.com", "www.handy.com", "www.github.com");
        for(int i = 0; i < addresses.size(); i++) {
            boolean success = addressCache.offerWithExpiration(InetAddress.getByName(addresses.get(i)),
                THREE_SECONDS * (isEvictionAscending ? (i + 1) : (addresses.size() - i)));
            assertTrue(success);
        }
        return addressCache;
    }
}
