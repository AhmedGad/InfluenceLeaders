package cache;

import graph.UserFollowers;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import javax.management.Query;

public class UsersLRUcache {
	private int capacity, initCap;
	AtomicInteger missCnt, total;
	HashMap<Long, UserFollowers> hashMap;
	Deque<Long> dequeue;

	/**
	 * 
	 * @param capacity
	 *            Maximum total number of followers in the cache
	 */
	public UsersLRUcache(int capacity) {
		this.capacity = capacity;
		initCap = capacity;
		dequeue = new LinkedList<Long>();
		hashMap = new HashMap<Long, UserFollowers>();
		missCnt = new AtomicInteger(0);
		total = new AtomicInteger(0);
	}

	public synchronized void add(Long key, UserFollowers value) {
		if (!hashMap.containsKey(key)) {
			hashMap.put(key, value);
			dequeue.addFirst(key);
			capacity -= value.size();
			while (capacity < 0) {
				capacity += hashMap.remove(dequeue.removeLast()).size();
			}
		}
	}

	public UserFollowers get(Long key) {
		UserFollowers res = hashMap.get(key);
		if (res == null)
			missCnt.incrementAndGet();
		total.incrementAndGet();
		return res;
	}

	public int getCacheSize() {
		return initCap - capacity;
	}

	public int getCachedUsers() {
		return hashMap.size();
	}

	public int getMissCnt() {
		return missCnt.intValue();
	}

	public int getTotalCnt() {
		return total.intValue();
	}
}
