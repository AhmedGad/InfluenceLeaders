package graph;

/**
 * interface for the Graph
 * 
 */
public interface Graph {
	/**
	 * check if user is exist or not
	 * 
	 * @param uid
	 *            user ID
	 * @return true if user exists, false if not
	 */
	public boolean exists(Long uid);

	/**
	 * 
	 * @param u1
	 *            user1
	 * @param u2
	 *            user2
	 * @return true if u1 is following u2, not the reverse
	 * @throws Exception
	 *            if u2 doesn't exist
	 */
	public boolean isFollowing(long uid1, long uid2) throws Exception;
}
