package graph;

import cache.UsersLRUcache;

/**
 *	the Graph Class that implements Graph interface 
 */

public class FollowingGraph implements Graph {
	public String dir;
	private UsersLRUcache cache;
	private final static int cacheSize = 200000000;

	public FollowingGraph(String dir) {
		this.dir = dir;
		cache = new UsersLRUcache(cacheSize);
	}

	@Override
	public boolean exists(long u) {
		return FollowersReader.userExists(u, dir);
	}

	/**
	 * @return true if u1 is following u2
	 * @throws Exception
	 *            if u2 doesn't exist
	 */
	@Override
	public boolean isFollowing(long u1, long u2) throws Exception {
		UserFollowers u2followers = cache.get(u2);
		if (u2followers == null) {
			u2followers = new UserFollowers(u2, FollowersReader.loadFollowers(
					u2, dir));
			cache.add(u2, u2followers);
		}
		return u2followers.hasFollower(u1);
	}
}
