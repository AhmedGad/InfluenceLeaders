package cache;

import graph.UserFollowers;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.ConcurrentHashMap;

public class UsersLRUcache {
	private int capacity;
	ConcurrentHashMap<Long, UserFollowers> hashMap;
	Deque<Long> dequeue;

	/**
	 * 
	 * @param capacity
	 *            Maximum total number of followers in the cache
	 */
	public UsersLRUcache(int capacity) {
		this.capacity = capacity;
		dequeue = new ArrayDeque<Long>();
		hashMap = new ConcurrentHashMap<Long, UserFollowers>();
	}

	public synchronized void add(Long key, UserFollowers value) {
		if (!hashMap.contains(key)) {
			hashMap.put(key, value);
			dequeue.addFirst(key);
			capacity -= value.size();
			while (capacity < 0) {
				capacity += hashMap.remove(dequeue.removeLast()).size();
			}
		}
	}

	public UserFollowers get(Long key) {
		return hashMap.get(key);
	}
}
