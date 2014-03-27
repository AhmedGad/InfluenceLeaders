package graph;

import java.io.File;
import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.util.HashMap;
import java.util.HashSet;

import cache.UsersLRUcache;

/**
 * the Graph Class that implements Graph interface
 */

public class FollowingGraph implements Graph {
	private static final String MAP_DIR = "UsersMapping";
	public String dir;
	public UsersLRUcache cache;
	private HashSet<Long> usersSet;
	private HashMap<Long, Integer> userMap;

	@SuppressWarnings("unchecked")
	public FollowingGraph(String dir, int cacheSize) {
		this.dir = dir;
		cache = new UsersLRUcache(cacheSize);
		usersSet = new HashSet<Long>();
		File[] fileList = new File(dir).listFiles();
		for (File file : fileList) {
			try {
				usersSet.add(Long.parseLong(file.getName()));
			} catch (Exception e) {
			}
		}
		try {
			FileInputStream fin = new FileInputStream(MAP_DIR);
			ObjectInputStream oos = new ObjectInputStream(fin);
			userMap = (HashMap<Long, Integer>) oos.readObject();
			oos.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public boolean exists(Long u) {
		return usersSet.contains(u);
	}

	/**
	 * @return true if u1 is following u2
	 * @throws Exception
	 *             if u2 doesn't exist
	 */
	@Override
	public boolean isFollowing(long u1, long u2) throws Exception {
		int u1I = userMap.get(u1), u2I = userMap.get(u2);
		UserFollowers u2followers = cache.get(u2I);
		if (u2followers == null) {
			u2followers = cache.get(u2I);
			if (u2followers == null) {
				u2followers = new UserFollowers(u2I,
						FollowersReader.loadFollowers(u2, dir));
				cache.add(u2I, u2followers);
			}
		}
		return u2followers.hasFollower(u1I);
	}
}
