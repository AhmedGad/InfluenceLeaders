package Tweets;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.TreeSet;

import twitter4j.Status;

public class UserIdExtractor {
	public static void main(String[] args) throws IOException, ClassNotFoundException {
		File statusDir = new File("./Status");
		File[] files = statusDir.listFiles();
		TreeSet<Long> idSet = new TreeSet<Long>();
		long total = 0;

		for (int i = 0; i < files.length; i++) {
			System.out.println(i + " " + files[i].getName());

			FileInputStream fin = new FileInputStream(files[i]);
			ObjectInputStream ois = new ObjectInputStream(fin);
			BufferedWriter bw = new BufferedWriter(new FileWriter("./UserIds/userID @ " + System.currentTimeMillis()
					+ ".txt"));

			while (true) {
				try {
					Status s = (Status) ois.readObject();
					bw.write(s.getUser().getId() + "");
					bw.newLine();

					idSet.add(s.getUser().getId());
					total++;
				} catch (Exception e) {
					fin.close();
					ois.close();
					bw.close();
					break;
				}
			}
		}

		System.out.println("Total tweets : " + total + ", unique ids : " + idSet.size());

	}
}
