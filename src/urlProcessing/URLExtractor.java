package urlProcessing;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map.Entry;

import cache.LRU;
import cache.LRUcache;

import twitter4j.Status;
import twitter4j.URLEntity;

/**
 * Extracts URL files from tweets files<br>
 * each URL file contains list of users that mentioned this url sorted by time<br>
 * Given tweets directory (input) and URL directory (output)
 */
public class URLExtractor {

	private static byte[] bytes;
	private static final String URLS_MAP_FILE = "UrlsMap";
	private static HashMap<String, ArrayList<Integer>> urlmap;
	// max size of sharing for a single url
	private final static int MAX_SIZE = 1500;
	private final static String inDirectory = "../../data/Status";
	// private final static String outDirectory = "../../data/URLs/";
	private final static int MAX_NUM_USERS = 240000000;

	// private static HashMap<String, String> urlMap;

	public static void main(String[] args) throws Exception {
		bytes = new byte[800000000];
		urlmap = new HashMap<String, ArrayList<Integer>>(4500000);
		HashMap<Long, Integer> map = readHashMap("UsersMapping");
		File dir = new File(inDirectory);
		int cnt = 0;
		long t1 = System.currentTimeMillis();
		int totalUsers = 0;

		File[] lista = dir.listFiles();
		// sorting in time so that the data is sorted in time
		Arrays.sort(lista, new Comparator<File>() {
			@Override
			public int compare(File o1, File o2) {
				return o1.getName().compareTo(o2.getName());
			}
		});
		System.out.println("Number of files : " + lista.length);
		for (File f : lista) {
			if (f.getName().endsWith(".txt")) {
				cnt++;
				System.out.println(cnt + " Read File : " + f.getName());
				FileInputStream fis;
				DataInputStream dis = new DataInputStream(
						fis = new FileInputStream(f));
				int size = dis.available();
				dis.read(bytes);

				ObjectInputStream ois = new ObjectInputStream(
						new ByteArrayInputStream(bytes));

				try {
					w: while (true) {
						Status status = (Status) ois.readObject();
						URLEntity[] urlEntity = status.getURLEntities();

						for (int i = 0; i < urlEntity.length; i++) {

							String url = urlEntity[i].getExpandedURL();
							if (!urlmap.containsKey(url))
								urlmap.put(url, new ArrayList<Integer>());

							ArrayList<Integer> list = urlmap.get(url);

							if (!map.containsKey(status.getUser().getId()))
								continue w;
							int id = map.get(status.getUser().getId());

							if (list.size() < MAX_SIZE) {
								list.add(id);
								totalUsers++;
							}
						}
					}
				} catch (Exception e) {
				}
				fis.close();
				dis.close();

			}
			System.out.println(totalUsers);
			if (totalUsers > MAX_NUM_USERS)
				break;
		}

		System.out.println("Finish Files");

		writeHashMap(URLS_MAP_FILE, urlmap);

		System.out.println("== FINISHED Successfully ==");

		System.out
				.println("Finished in : " + (System.currentTimeMillis() - t1));

	}

	private static void writeHashMap(String directory, Object map)
			throws IOException {
		FileOutputStream fout = new FileOutputStream(directory);
		ObjectOutputStream oos = new ObjectOutputStream(fout);
		oos.writeObject(map);

		oos.close();
		fout.close();
	}

	private static HashMap<Long, Integer> readHashMap(String directory)
			throws IOException, ClassNotFoundException {
		FileInputStream fin;
		try {
			fin = new FileInputStream(directory);
		} catch (FileNotFoundException e) {
			writeHashMap(directory, new HashMap<Long, Integer>());
			fin = new FileInputStream(directory);
		}

		ObjectInputStream ois = new ObjectInputStream(fin);
		@SuppressWarnings("unchecked")
		HashMap<Long, Integer> map = (HashMap<Long, Integer>) ois.readObject();
		ois.close();

		return map;
	}

	// private static void writeEntry(String key, ArrayList<String> value)
	// throws IOException {
	// if (!urlMap.containsKey(key)) {
	// urlMap.put(key, urlMap.size() + "");
	// }
	//
	// String fileName = urlMap.get(key);
	//
	// FileWriter fw = new FileWriter(outDirectory + fileName, true);
	// for (String s : value) {
	// fw.write(s + "\n");
	// }
	// fw.close();
	// }
}