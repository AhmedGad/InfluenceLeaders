package Tweets;

import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.HashMap;
import java.util.TreeMap;

import DataSummary.UserData;

public class UsersReader {
	private static final String MAP_DIR = "UsersMapping";
	private static UsersReader instance = null;
	private TreeMap<Integer, UserData> tr;
	private HashMap<Long, Integer> userMap;

	// This is not thread safe, just sayin
	public static UsersReader getInstance() throws IOException {
		if (instance == null)
			instance = new UsersReader();
		return instance;
	}

	private UsersReader() throws IOException {
		try {
			FileInputStream fin = new FileInputStream(MAP_DIR);
			ObjectInputStream oos = new ObjectInputStream(fin);
			userMap = (HashMap<Long, Integer>) oos.readObject();
			oos.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

		tr = new TreeMap<Integer, UserData>();
		FileInputStream fin = new FileInputStream("UserObjects");
		ObjectInputStream ois = new ObjectInputStream(fin);
		while (true) {
			try {
				UserData s = (UserData) ois.readObject();
				if (userMap.containsKey(s.id))
					tr.put(userMap.get(s.id), s);
			} catch (Exception e) {
				fin.close();
				ois.close();
				break;
			}
		}
		System.out.println("total users in users map tree "+tr.size());
	}

	public UserData getUser(Integer uid) {
		return tr.get(uid);
	}

	public static void main(String[] args) throws IOException {
		UsersReader.getInstance();
	}
}
