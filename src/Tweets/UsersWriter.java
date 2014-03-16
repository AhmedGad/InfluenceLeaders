package Tweets;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashSet;

import DataSummary.UserData;

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
		FileOutputStream fout = new FileOutputStream(outputFile);
		ObjectOutputStream oos = new ObjectOutputStream(fout);
		for (File file : fileList) {
			System.out.println(file.getAbsolutePath());
			ObjectInputStream ois = new ObjectInputStream(new FileInputStream(
					file));
			while (true) {
				try {
					User user = ((Status) ois.readObject()).getUser();
					if (!set.contains(user.getId())) {
						UserData u = new UserData(user.getId(),user.getFollowersCount(), user.getFriendsCount(), user.getCreatedAt());
						set.add(user.getId());
						oos.writeObject(u);
						if (set.size() % 10000 == 0) {
							oos.flush();
							oos.reset();
						}
						if (set.size() % 100000 == 0) {
//							oos.close();
//							fout.close();
//
//							fout = new FileOutputStream(outputFile);
//							oos = new ObjectOutputStream(fout);
							System.out.println("Finished "+set.size());
						}
					}
				} catch (Exception e) {
					ois.close();
					break;
				}
			}
		}
		oos.flush();
		oos.close();
		System.out.println(set.size());
		return set.size();
	}

	public static void main(String[] args) throws IOException {
		UsersWriter uw = new UsersWriter(new File("./Status").listFiles(),
				new File("UserObjects"));
		System.out.println(uw.writeUsers());
	}
}
