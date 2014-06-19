package dataPreparation;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;

public class ConvertUrlsMaptoSet {
	public static void main(String[] args) throws ClassNotFoundException,
			IOException {
		HashMap<String, ArrayList<Integer>> map = readUrls("UrlsMap");
		System.out.println(map.size());
		int num_segments = 5;
		ArrayList<Integer>[] list = new ArrayList[map.size() / num_segments];
		int top = 0;
		int cur = 0;
		File f = new File("UrlsSet");
		if (!f.isDirectory())
			f.mkdir();
		for (Entry<String, ArrayList<Integer>> e : map.entrySet()) {
			list[top++] = e.getValue();
			if (top == list.length) {
				writeSet("./UrlsSet/" + cur, list);
				cur++;
				top = 0;
				list = new ArrayList[map.size() / num_segments];
			}
		}
	}

	private static HashMap<String, ArrayList<Integer>> readUrls(String directory)
			throws IOException, ClassNotFoundException {
		FileInputStream fin = new FileInputStream(directory);
		ObjectInputStream ois = new ObjectInputStream(fin);
		@SuppressWarnings("unchecked")
		HashMap<String, ArrayList<Integer>> map = (HashMap<String, ArrayList<Integer>>) ois
				.readObject();
		ois.close();
		return map;
	}

	private static void writeSet(String directory, Object map)
			throws IOException {
		FileOutputStream fout = new FileOutputStream(directory);
		ObjectOutputStream oos = new ObjectOutputStream(fout);
		oos.writeObject(map);

		oos.close();
		fout.close();
	}
}
