package cache;

import java.util.LinkedList;
import java.util.Map.Entry;

public interface LRU<S extends Comparable<S>, T> {
	public void setSize(int size);

	/**
	 * add key and value
	 * 
	 * @param key
	 * @param value
	 * @return null if LRU size less than the max size, else return least
	 *         recently used object and this object should be written to file
	 */
	public Entry<S, T> add(S key, T value);

	/**
	 * remove object from LRU set
	 * 
	 * @param key
	 */
	public void remove(S key);

	/**
	 * get key's Entry
	 * 
	 * @param key
	 * @return object or null if it doesn't exist
	 */
	public T get(S key);

	/**
	 * return entry set
	 * 
	 * @return
	 */
	public LinkedList<Entry<S, T>> getEntrySet();
}
