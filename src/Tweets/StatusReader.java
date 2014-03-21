package Tweets;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.util.HashMap;
import java.util.Map.Entry;

import twitter4j.Status;
import twitter4j.URLEntity;

/**
 * read status files in efficient way
 *
 */
public class StatusReader {

	static byte[] bytes;;
	static HashMap<String, Integer> hash = new HashMap<String, Integer>();

	public static void main(String[] args) throws Exception {
		bytes = new byte[200000000];
		File dir = new File("./Status");
		int totalTweets = 0;
		for (File f : dir.listFiles()) {
			System.out.println(f.getName());
			
			if (f.getName().endsWith(".txt")) {
				long t1 = System.currentTimeMillis();
				FileInputStream fis;
				DataInputStream dis = new DataInputStream(fis = new FileInputStream(f));
				int size = dis.available();
				System.out.println(size + " " + dis.read(bytes));

				ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bytes));

				try {
					while (true) {
						Status status = (Status) ois.readObject();
						URLEntity[] urlEntity = status.getURLEntities();
						totalTweets++;

						for (int i = 0; i < urlEntity.length; i++) {
							String s = urlEntity[i].getExpandedURL();
							if (hash.containsKey(s)) {
								hash.put(s, hash.get(s) + 1);
							}else{
								hash.put(s, 1);
							}
						}
					}
				} catch (Exception e) {
//					System.out.println(System.currentTimeMillis() - t1);
				}

				fis.close();
				dis.close();
			}
		}
		
		int max = 0, min = Integer.MAX_VALUE, sum = 0;
		for(Entry<String, Integer> set: hash.entrySet()){
			sum += set.getValue();
			max = Math.max(max, set.getValue());
			min = Math.min(min, set.getValue());
			if(set.getValue() == 89578)
				System.out.println("URL : " + set.getKey());
		}
		
		System.out.println("Average : " + sum * 1.0/ hash.size());
		System.out.println("Max : " + max);
		System.out.println("Min : " + min);
		System.out.println("Total tweets : " + totalTweets);
	}
}