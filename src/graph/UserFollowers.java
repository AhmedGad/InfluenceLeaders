package graph;

import java.util.Arrays;
import graphBuilder.MyIntegerArrayList;

/**
 * contains user's Followers
 */
public class UserFollowers {

	private int uid;
	private int[] followers;

	public UserFollowers(int uid, MyIntegerArrayList followersList) {
		this.uid = uid;
		followers = new int[followersList.size()];
		for (int i = 0; i < followersList.size(); i++) {
			followers[i] = followersList.get(i);
		}
		Arrays.sort(followers);
	}

	public boolean hasFollower(int uid) {
		return Arrays.binarySearch(followers, uid) >= 0;
	}

	public long getUid() {
		return uid;
	}

	public int size() {
		return followers.length;
	}
}
