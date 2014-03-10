package graphBuilder;

import java.util.ArrayList;

public class MyLongArrayList {

	private ArrayList<long[]> list;
	private int listIndx, arrIndx;
	private final int len = 200;

	public MyLongArrayList() {
		list = new ArrayList<long[]>();
		list.add(new long[len]);
		listIndx = 0;
		arrIndx = 0;
	}

	public void add(long n) {
		if (arrIndx == len) {
			arrIndx = 0;
			listIndx++;
			list.add(new long[len]);
		}
		list.get(listIndx)[arrIndx] = n;
		arrIndx++;
	}

	public long get(int i) {
		return list.get(i / len)[i % len];
	}

	public int size() {
		return listIndx * len + arrIndx;
	}
}
