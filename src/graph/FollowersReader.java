package graph;

import graphBuilder.MyIntegerArrayList;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

/**
 * read followers for specific user
 */
public class FollowersReader {
	/**
	 * 
	 * @param uid
	 *            long because file names are the actual ID not the maped one
	 * @param dir
	 * @return
	 * @throws Exception
	 */
	public static MyIntegerArrayList loadFollowers(long uid, String dir)
			throws Exception {

		BufferedReader buff = new BufferedReader(new FileReader(new File(dir
				+ uid)));
		MyIntegerArrayList res = new MyIntegerArrayList();
		String s;
		while ((s = buff.readLine()) != null) {
			res.add(Integer.parseInt(s));
		}
		buff.close();
		return res;
	}
}
