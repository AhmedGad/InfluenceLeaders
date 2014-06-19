package graph;

import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.util.HashMap;
import java.util.TreeMap;

/**
 * the Graph Class that implements Graph interface
 */

public class FollowingGraph implements Graph {
	public String dir;
	
	private TreeMap<Integer, UserFollowers> map;

	@SuppressWarnings("unchecked")
	public FollowingGraph(String dir) {
		this.dir = dir;
		try {
			FileInputStream fin = new FileInputStream(dir);
			ObjectInputStream oos = new ObjectInputStream(fin);
			map = (TreeMap<Integer, UserFollowers>) oos.readObject();
			oos.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public boolean exists(int u) {
		return map.containsKey(u);
	}

	/**
	 * @return true if u1 is following u2
	 * @throws Exception
	 *             if u2 doesn't exist
	 */
	@Override
	public boolean isFollowing(int u1, int u2) throws Exception {
		int u1I = u1, u2I = u2;
		UserFollowers u2followers = map.get(u2I);
		return u2followers.hasFollower(u1I);
	}
}
