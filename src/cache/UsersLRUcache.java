package cache;

import graph.UserFollowers;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

public class UsersLRUcache {
	private int capacity;
	ConcurrentHashMap<Long, UserFollowers> hm;
	ConcurrentLinkedDeque<Long> queue;

	/**
	 * 
	 * @param capacity
	 *            Maximum total number of followers in the cache
	 */
	public UsersLRUcache(int capacity) {
		this.capacity = capacity;
		queue = new ConcurrentLinkedDeque<Long>();
		hm = new ConcurrentHashMap<Long, UserFollowers>();
	}

	public void add(Long key, UserFollowers value) {
		hm.put(key, value);
		capacity -= value.size();
		while (capacity < 0) {
			capacity += hm.remove(queue.removeLast()).size();
		}
	}

	public UserFollowers get(Long key) {
		return hm.get(key);
	}
}
