package DataSummary;

import graph.FollowingGraph;
import graph.Graph;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.Map.Entry;

import twitter4j.User;

import Tweets.UsersReader;

public class TotalData {

	private final static String urlsDir = "./URLs";
	private final static String outDirectory = "./Final/";
	private final static TreeMap<Long, UserNode> tr = new TreeMap<Long, UserNode>();

	public static void main(String[] args) throws Exception {
		File dir = new File(urlsDir);
		Graph graph = new FollowingGraph("./Graph/");
		System.out.println("Total Files: " + dir.listFiles().length);
		int done = 0;
		HashSet<Long> hs = new HashSet<Long>();
		for (File f : dir.listFiles()) {
			BufferedReader in = null;
			try {
				ArrayList<String> lines = new ArrayList<String>();
				in = new BufferedReader(new FileReader(f));
				while (true) {
					String s = in.readLine();
					if (s == null)
						break;
					lines.add(s);
				}
				if (lines.size() > 2000) {
					System.out.println("Ignoring file " + f.getName()
							+ " with size " + lines.size());
					continue;
				}
				HashSet<Long> completedUsers = new HashSet<Long>();
				TreeMap<Long, Integer> local = new TreeMap<Long, Integer>();
				TreeMap<Long, Integer> total = new TreeMap<Long, Integer>();
				TreeMap<Long, Long> parent = new TreeMap<Long, Long>();
				for (int i = 0; i < lines.size(); i++) {
					StringTokenizer st = new StringTokenizer(lines.get(i), ":");
					long fromId = Long.parseLong(st.nextToken());
					hs.add(fromId);
					if (!completedUsers.contains(fromId)) {
						if (!tr.containsKey(fromId))
							tr.put(fromId, new UserNode());
						tr.get(fromId).tweets++;
						completedUsers.add(fromId);
					} else
						continue; // Ignore repetitions

					for (int j = i + 1; j < lines.size(); j++) {
						st = new StringTokenizer(lines.get(j), ":");
						long toId = Long.parseLong(st.nextToken());

						if (completedUsers.contains(toId))
							continue; // Ignore repetitions
						if (parent.containsKey(toId))
							continue; // credit already given to a parent
										// (Assuming First Influence)
						boolean follow = false;
						try {
							follow = graph.isFollowing(toId, fromId);
						} catch (Exception e) {
							// user doesn't exist
							follow = false;
						}
						// follow is always false !!!
						if (follow) {
							System.out.println(follow);
							parent.put(toId, fromId);
							if (!local.containsKey(fromId))
								local.put(fromId, 0);
							local.put(fromId, local.get(fromId) + 1);
						}
					}
				}
				for (int i = lines.size() - 1; i >= 0; i--) {
					StringTokenizer st = new StringTokenizer(lines.get(i), ":");
					long child = Long.parseLong(st.nextToken());
					if (parent.containsKey(child)) {
						long parentId = parent.get(child);
						if (!total.containsKey(parentId))
							total.put(parentId, 0);
						if (local.containsKey(child)) // zero if no cascades
							total.put(parentId,
									total.get(parentId) + local.get(child));
					}
				}
				for (Entry<Long, Integer> e : local.entrySet()) {
					if (!tr.containsKey(e.getKey()))
						tr.put(e.getKey(), new UserNode());
					UserNode u = tr.get(e.getKey());
					u.localInfSum = e.getValue();
					u.totalInfSum = e.getValue();
					u.minLocalInf = Math.min(u.minLocalInf, e.getValue());
					u.maxLocalInf = Math.max(u.maxLocalInf, e.getValue());
				}
				for (Entry<Long, Integer> e : total.entrySet()) {
					if (!tr.containsKey(e.getKey()))
						tr.put(e.getKey(), new UserNode());
					UserNode u = tr.get(e.getKey());
					u.totalInfSum += e.getValue();
					u.minTotalInf = Math.min(u.minTotalInf, e.getValue());
					u.maxTotalInf = Math.max(u.maxTotalInf, e.getValue());
				}
				done++;
				if (done % 1000 == 0) {
					System.out.println("Finished Files: " + done);
				}
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				if (in != null)
					in.close();
			}
		}
		System.out.println(tr.size());
		UsersReader reader = UsersReader.getInstance();
		FileWriter fw = new FileWriter(outDirectory + "TotalData.txt", false);
		for (Entry<Long, UserNode> e : tr.entrySet()) {
			UserData u = reader.getUser(e.getKey());
			if (u != null) {
				StringBuilder sb = new StringBuilder();
				sb.append(u.followers + ",");
				sb.append(u.friends + ",");
				sb.append(e.getValue().tweets + ",");
				sb.append(u.dateJoined + ",");
				sb.append((float) e.getValue().totalInfSum
						/ e.getValue().tweets + ",");
				sb.append(e.getValue().minTotalInf + ",");
				sb.append(e.getValue().maxTotalInf + ",");

				sb.append((float) e.getValue().localInfSum
						/ e.getValue().tweets + ",");
				sb.append(e.getValue().minLocalInf + ",");
				sb.append(e.getValue().maxLocalInf + "\n");
				fw.write(sb.toString());
			}
		}
		fw.close();
	}

	static class UserNode {
		// total number of tweets ( different urls posted )
		int tweets = 0;
		// average total is totalInfSum/tweets
		int totalInfSum = 0;
		// average local is localInfSum/tweets
		int localInfSum = 0;

		int minTotalInf = Integer.MAX_VALUE;
		int minLocalInf = Integer.MAX_VALUE;

		int maxTotalInf = Integer.MIN_VALUE;
		int maxLocalInf = Integer.MIN_VALUE;
	}
}
