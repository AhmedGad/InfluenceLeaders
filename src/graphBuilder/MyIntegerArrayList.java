package graphBuilder;

import java.util.ArrayList;

public class MyIntegerArrayList {

	private ArrayList<int[]> list;
	private int listIndx, arrIndx;
	private final int len = 200;

	public MyIntegerArrayList() {
		list = new ArrayList<int[]>();
		list.add(new int[len]);
		listIndx = 0;
		arrIndx = 0;
	}

	public void add(int n) {
		if (arrIndx == len) {
			arrIndx = 0;
			listIndx++;
			list.add(new int[len]);
		}
		list.get(listIndx)[arrIndx] = n;
		arrIndx++;
	}

	public int get(int i) {
		return list.get(i / len)[i % len];
	}

	public int size() {
		return listIndx * len + arrIndx;
	}
}
