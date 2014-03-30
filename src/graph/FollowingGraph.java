package graph;

import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.util.HashMap;
import java.util.TreeMap;


/**
 * the Graph Class that implements Graph interface
 */

public class FollowingGraph implements Graph {
	private static final String MAP_DIR = "UsersMapping";
	public String dir;
	private HashMap<Long, Integer> userMap;
	private TreeMap<Integer, UserFollowers> map;

	@SuppressWarnings("unchecked")
	public FollowingGraph(String dir) {
		this.dir = dir;
		try {
			FileInputStream fin = new FileInputStream(MAP_DIR);
			ObjectInputStream oos = new ObjectInputStream(fin);
			userMap = (HashMap<Long, Integer>) oos.readObject();
			oos.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
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
	public boolean exists(Long u) {
		return userMap.containsKey(u) && map.containsKey(userMap.get(u));
	}

	/**
	 * @return true if u1 is following u2
	 * @throws Exception
	 *             if u2 doesn't exist
	 */
	@Override
	public boolean isFollowing(long u1, long u2) throws Exception {
		int u1I = userMap.get(u1), u2I = userMap.get(u2);
		UserFollowers u2followers = map.get(u2I);
		return u2followers.hasFollower(u1I);
	}
}
