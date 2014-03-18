package graph;

import java.io.File;
import java.util.HashSet;

import cache.UsersLRUcache;

/**
 * the Graph Class that implements Graph interface
 */

public class FollowingGraph implements Graph {
	public String dir;
	private UsersLRUcache cache;
	private HashSet<Long> usersSet;

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
		UserFollowers u2followers = cache.get(u2);
		if (u2followers == null) {
			u2followers = cache.get(u2);
			if (u2followers == null) {
				u2followers = new UserFollowers(u2,
						FollowersReader.loadFollowers(u2, dir));
				cache.add(u2, u2followers);
			}
		}
		return u2followers.hasFollower(u1);
	}
}
