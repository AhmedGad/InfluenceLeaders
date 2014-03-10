package cache;

import graph.UserFollowers;

import java.util.Map.Entry;

public class UsersLRUcache extends LRUcache<Long, UserFollowers> {
	/**
	 * 
	 * @param capacity
	 *            Maximum total number of followers in the cache
	 */
	public UsersLRUcache(int capacity) {
		super(capacity);
	}

	@Override
	public Entry<Long, UserFollowers> add(Long key, UserFollowers value) {
		if (tr.containsKey(key)) {
			Node n = ls.remove(tr.get(key));
			ls.add(n);
			n.value = value;
		} else {
			Node n = new Node(key, value);
			ls.add(n);
			tr.put(key, n);
			while (value.size() > size) {
				n = tr.remove(ls.removeLast().key);
				size += n.value.size();
			}
			size -= value.size();
		}
		return null;
	}

	@Override
	public void remove(Long key) {
		UserFollowers n = super.get(key);
		size += n.size();
		super.remove(key);
	}

}
