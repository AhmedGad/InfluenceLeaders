package graph;

import graphBuilder.MyLongArrayList;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

/**
 * read followers for specific user
 */
public class FollowersReader {

	public static MyLongArrayList loadFollowers(long uid, String dir)
			throws Exception {
		
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
}
