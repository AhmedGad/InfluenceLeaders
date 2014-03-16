package Tweets;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashSet;

import twitter4j.Status;
import twitter4j.User;

public class UsersWriter {
	private File[] fileList;
	private File outputFile;
	private HashSet<Long> set;

	public UsersWriter(File[] statusFileList, File outputFile) {
		this.fileList = statusFileList;
		this.outputFile = outputFile;
	}

	/**
	 * 
	 * @return number of unique written users
	 * 
	 */
	public int writeUsers() throws IOException {
		set = new HashSet<Long>();
		ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(
				outputFile));
		for (File file : fileList) {
			System.out.println(file.getAbsolutePath());
			ObjectInputStream ois = new ObjectInputStream(new FileInputStream(
					file));
			while (true) {
				try {
					User user = ((Status) ois.readObject()).getUser();
					if (!set.contains(user.getId())) {
						set.add(user.getId());
						oos.writeObject(user);
						if (set.size() % 10000 == 0) {
							oos.reset();
							System.out.println(set.size() + " users written.");
						}
						if (set.size() == 250000) {
							oos.close();
							System.exit(0);
						}
					}
				} catch (Exception e) {
					ois.close();
					break;
				}
			}
		}
		oos.close();
		return set.size();
	}

	public static void main(String[] args) throws IOException {
		UsersWriter uw = new UsersWriter(new File("./status").listFiles(),
				new File("users"));
		System.out.println(uw.writeUsers());
	}
}
