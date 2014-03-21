package dataPreparation;

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.PrintWriter;
import java.util.HashSet;

import twitter4j.Status;
import twitter4j.User;

public class ActiveUserExtractor {
	private String statusesDir;
	private File outFile;
	private byte buffArr[];
	private HashSet<Long> set;

	public ActiveUserExtractor(String statusesDir, File outFile,
			int maxStatusFileSize) {
		this.statusesDir = statusesDir;
		this.outFile = outFile;
		buffArr = new byte[maxStatusFileSize];
		set = new HashSet<Long>();
	}

	public void start() throws IOException {
		File[] fileList = new File(statusesDir).listFiles();
		PrintWriter errorLogWriter = new PrintWriter(new File(
				"./logs/ActiveUserExtractor-error-log.txt"));
		PrintWriter logWriter = new PrintWriter(
				"./logs/ActiveUserExtractor-log.txt");
		BufferedWriter writer = new BufferedWriter(new FileWriter(outFile));
		for (File file : fileList) {
			System.out.println(file.getName() + " -> started");
			logWriter.println(file.getName() + " -> started");
			logWriter.flush();

			FileInputStream fis = new FileInputStream(file);
			DataInputStream dis = new DataInputStream(fis);
			int size = dis.read(buffArr);
			if (dis.available() > 0) {
				System.err.println(file.getName() + " not red completely!!");
				errorLogWriter
						.println(file.getName() + " not red completely!!");
				errorLogWriter.flush();
			}
			ObjectInputStream ois = new ObjectInputStream(
					new ByteArrayInputStream(buffArr, 0, size));
			int statusesCnt = 0, newUsers = 0;
			while (true) {
				try {
					User user = ((Status) ois.readObject()).getUser();
					statusesCnt++;
					if (!set.contains(user.getId())) {
						set.add(user.getId());
						writer.write(user.getId() + "\n");
						newUsers++;
					}
				} catch (Exception e) {
					if (!e.toString().equals("java.io.EOFException")) {
						e.printStackTrace();
						errorLogWriter.println(file.getName()
								+ " throws Exception: " + e.toString());
						errorLogWriter.flush();
					}
					break;
				}
			}
			System.out.println(file.getName()
					+ "->  finished,\tNum of tweets: " + statusesCnt
					+ ", Num of new Users: " + newUsers
					+ ", total num of users: " + set.size() + "\n");
			logWriter.println(file.getName() + "->  finished,\tNum of tweets: "
					+ statusesCnt + ", Num of new Users: " + newUsers
					+ ", total num of users: " + set.size() + "\n");
			logWriter.flush();
			fis.close();
			dis.close();
		}

		System.out
				.println("\n\n*****\tFINISHED! .. Total Num of Active Users: "
						+ set.size());
		logWriter
				.println("\n\n*****\tFINISHED! .. Total Num of Active Users: "
						+ set.size() + "\n");
		logWriter.flush();

		writer.close();
		errorLogWriter.close();
		logWriter.close();
	}

	public static void main(String[] args) throws IOException {
		if (!new File("./logs/").exists()) {
			new File("./logs/").mkdir();
		}
		ActiveUserExtractor extractor = new ActiveUserExtractor("./Statuses/",
				new File("activeUser.txt"), 200000000);
		extractor.start();
	}
}
