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

	private static byte[] bytes = new byte[200000000];
	private static LRU<String, ArrayList<String>> urlSet;
	private final static int MAX_SIZE = 50;
	private final static String inDirectory = "./Status";
	private final static String outDirectory = "./URLs/";
	private static HashMap<String, String> urlMap;

	public static void main(String[] args) throws Exception {
		urlSet = new LRUcache<String, ArrayList<String>>(10000);
		urlMap = readHashMap(inDirectory + "/urlMap");

		File dir = new File(inDirectory);
		int cnt = 0;
		long t1 = System.currentTimeMillis();
		int hit = 0, miss = 0, totalURLs = 0, removed = 0, reachMax = 0;
		int statusCnt = 0;

		for (File f : dir.listFiles()) {
			if (f.getName().endsWith(".txt")) {
				cnt++;
				FileInputStream fis;
				DataInputStream dis = new DataInputStream(
						fis = new FileInputStream(f));
				int size = dis.available();
				System.out.println(size + " " + dis.read(bytes));

				ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bytes));

				try {
					while (true) {
						statusCnt++;
						if (statusCnt % 1000 == 0)
							System.out.println(statusCnt);

						Status status = (Status) ois.readObject();
						URLEntity[] urlEntity = status.getURLEntities();
						// System.out.println(xx++);

						for (int i = 0; i < urlEntity.length; i++) {
							totalURLs++;

							String url = urlEntity[i].getExpandedURL();
							ArrayList<String> list = urlSet.get(url);
							if (list == null) { // not found
								miss++;
								list = new ArrayList<String>();
								Entry<String, ArrayList<String>> oldEntry = urlSet
										.add(url, list);

								if (oldEntry != null) {
									removed++;

									// write to file
									writeEntry(oldEntry.getKey(),
											oldEntry.getValue());
									list.remove(oldEntry.getKey());
								}
							} else {
								hit++;
							}

							String str = status.getUser().getId() + ":"
									+ status.getCreatedAt().toString();
							list.add(str);
							if (list.size() >= MAX_SIZE) {
								reachMax++;

								// write to file
								writeEntry(url, list);
								// remove from urlSet
								urlSet.remove(url);
							}
						}
					}
				} catch (Exception e) {
					// e.printStackTrace();
					// System.out.println("Finish file : " + f.getName() +
					// ", in : " + (System.currentTimeMillis() - t1));
					// System.out.printf("Total URLs : %d, Hit : %d, Miss : %d\n",
					// totalURLs, hit, miss);
				}
				fis.close();
				dis.close();

				if (cnt == 1)
					break;

				writeHashMap(inDirectory + "/urlMap", urlMap);
			}
		}

		System.out.println("Finish Files");

		// write remaining entries
		LinkedList<Entry<String, ArrayList<String>>> list = urlSet
				.getEntrySet();
		System.out.println("cache size : " + list.size());
		for (Entry<String, ArrayList<String>> entry : list) {
			writeEntry(entry.getKey(), entry.getValue());
		}

		writeHashMap(inDirectory + "/urlMap", urlMap);
		System.out.println("== FINISHED Successfully ==");

		System.out.println("Finish file in : "
				+ (System.currentTimeMillis() - t1));
		System.out
				.printf("Total URLs : %d, Hit : %d, Miss : %d, Removed from cache : %d, Reach Max(%d) : %d\n",
						totalURLs, hit, miss, removed, MAX_SIZE, reachMax);

	}

	private static void writeHashMap(String directory,
			HashMap<String, String> map) throws IOException {
		FileOutputStream fout = new FileOutputStream(directory);
		ObjectOutputStream oos = new ObjectOutputStream(fout);
		oos.writeObject(map);

		oos.close();
		fout.close();
	}

	private static HashMap<String, String> readHashMap(String directory)
			throws IOException, ClassNotFoundException {
		FileInputStream fin;
		try {
			fin = new FileInputStream(directory);
		} catch (FileNotFoundException e) {
			writeHashMap(directory, new HashMap<String, String>());
			fin = new FileInputStream(directory);
		}

		ObjectInputStream ois = new ObjectInputStream(fin);
		@SuppressWarnings("unchecked")
		HashMap<String, String> map = (HashMap<String, String>) ois
				.readObject();
		ois.close();

		return map;
	}

	private static void writeEntry(String key, ArrayList<String> value)
			throws IOException {
		if (!urlMap.containsKey(key)) {
			urlMap.put(key, urlMap.size() + "");
		}

		String fileName = urlMap.get(key);

		FileWriter fw = new FileWriter(outDirectory + fileName, true);
		for (String s : value) {
			fw.write(s + "\n");
		}
		fw.close();
	}
}