package cache;

import java.util.AbstractMap;
import java.util.LinkedList;
import java.util.Map.Entry;
import java.util.TreeMap;

public class LRUcache<S extends Comparable<S>, T> implements LRU<S, T> {
	protected int size;
	protected TreeMap<S, Node> tr;
	protected linkedList ls;

	public LRUcache(int capacity) {
		size = capacity;
		ls = new linkedList();
		tr = new TreeMap<S, Node>();
	}

	@Override
	public void setSize(int size) {
		this.size = size;
	}

	@Override
	public Entry<S, T> add(S key, T value) {
		if (tr.containsKey(key)) {
			Node n = ls.remove(tr.get(key));
			ls.add(n);
			n.value = value;
			return null;
		} else {
			Node n = new Node(key, value);
			ls.add(n);
			tr.put(key, n);
			if (tr.size() > size) {
				n = tr.remove(ls.removeLast().key);
				return new AbstractMap.SimpleEntry<S, T>(n.key, n.value);
			} else {
				return null;
			}
		}
	}

	@Override
	public void remove(S key) {
		ls.remove(tr.get(key));
		tr.remove(key);
	}

	@Override
	public T get(S key) {
		if (tr.containsKey(key)) {
			Node n = ls.remove(tr.get(key));
			ls.add(n);
			return n.value;
		} else {
			// not found
			return null;
		}
	}

	@Override
	public LinkedList<Entry<S, T>> getEntrySet() {
		LinkedList<Entry<S, T>> lst = new LinkedList<>();
		Node n = ls.head;
		while (n != null) {
			lst.add(new AbstractMap.SimpleEntry<S, T>(n.key, n.value));
			n = n.next;
		}
		return lst;
	}

	public class linkedList {
		Node head;
		Node tail;

		public void add(Node n) {
			if (head == null) {
				head = tail = n;
			} else {
				tail.next = n;
				n.prev = tail;
				tail = tail.next;
				n.next = null;
			}
		}

		public Node remove(Node n) {
			if (n == tail && n == head) {
				tail = head = null;
			} else if (n == tail) {
				tail.prev.next = null;
				tail = tail.prev;
			} else if (n == head) {
				head = head.next;
			} else {
				n.prev.next = n.next;
				n.next.prev = n.prev;
			}
			return n;
		}

		public Node removeLast() {
			Node tmp = head;
			head = head.next;
			return tmp;
		}
	}

	public class Node {
		Node prev;
		Node next;
		public S key;
		public T value;

		public Node(S k, T v) {
			key = k;
			value = v;
		}
	}

}
