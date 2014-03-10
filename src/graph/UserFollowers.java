package graph;

import java.util.Arrays;

import graphBuilder.MyLongArrayList;

public class UserFollowers {

	private long uid;
	private long[] followers;

	public UserFollowers(long uid, MyLongArrayList followersList) {
		this.uid = uid;
		followers = new long[followersList.size()];
		for (int i = 0; i < followersList.size(); i++) {
			followers[i] = followersList.get(i);
		}
		Arrays.sort(followers);
	}

	public boolean hasFollower(long uid) {
		return Arrays.binarySearch(followers, uid) >= 0;
	}

	public long getUid() {
		return uid;
	}

	public int size() {
		return followers.length;
	}
}
