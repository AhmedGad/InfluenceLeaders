package Tweets;

import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.TreeMap;

import DataSummary.UserData;


public class UsersReader {
	private static UsersReader instance = null;
	private TreeMap<Long, UserData> tr;
	
	// This is not thread safe, just sayin
	public static UsersReader getInstance() throws IOException{
		if(instance == null)
			instance = new UsersReader();
		return instance;
	}
	
	private UsersReader() throws IOException  {
		tr = new TreeMap<Long, UserData>();
		FileInputStream fin = new FileInputStream("UserObjects");
		ObjectInputStream ois = new ObjectInputStream(fin);
		int total= 0;
		while (true) {
			try {
				UserData s = (UserData) ois.readObject();
				tr.put(s.id,s);
				total++;
			} catch (Exception e) {
				fin.close();
				ois.close();
				break;
			}
		}
		System.out.println(total);
	}

	public UserData getUser(Long uid) {
		return tr.get(uid);
	}
	public static void main(String[] args) throws IOException {
		UsersReader.getInstance();
	}
}
