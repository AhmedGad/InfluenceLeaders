package graph;

import graphBuilder.MyLongArrayList;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

/**
 * read followers for specific user
 */
public class FollowersReader {

	public static MyLongArrayList loadFollowers(long uid, String dir) throws Exception {
		if (!userExists(uid, dir)) {
			throw new Exception("Followed user " + uid + " doesnot exist");
		}
		BufferedReader buff = new BufferedReader(new FileReader(new File(dir
				+ uid)));
		MyLongArrayList res = new MyLongArrayList();
		String s;
		while ((s = buff.readLine()) != null) {
			res.add(Long.parseLong(s));
		}
		buff.close();
		return res;
	}

	public static boolean userExists(long uid, String dir) {
		return new File(dir + uid).exists();
	}
}
