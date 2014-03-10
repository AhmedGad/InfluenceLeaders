package graph;
public interface Graph {
	public boolean exists(long uid);

	/**
	 * 
	 * @param u1
	 * @param u2
	 * @return true if u1 is following u2
	 */
	public boolean isFollowing(long uid1, long uid2) throws Exception;
}
