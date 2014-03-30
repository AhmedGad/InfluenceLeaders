package DataSummary;

import graph.FollowingGraph;
import graph.Graph;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import Tweets.UsersReader;

public class TotalData {

	private final static String urlsDir = "../../data/URLs";
	private final static String outDirectory = "./Final/";
	private final static String usersDirectory = "../../data/Users-trimmed6/";
	private static ConcurrentHashMap<Long, UserNode> tr;
	private static ConcurrentHashMap<String, String> finishedFiles;
	private static AtomicInteger done = new AtomicInteger(0);
	private static final int NUM_THREADS = 30;
	// private static final ArrayBlockingQueue<Graph> graphs = new
	// ArrayBlockingQueue<Graph>(
	// NUM_THREADS + 1);
	private static final FollowingGraph graph = new FollowingGraph("graphMap");

	public static void main(String[] args) throws Exception {
		finishedFiles = readHashMap("finishedFiles");
		tr = readUserMap("finishedUsers");
		System.out.println(finishedFiles.size());
		System.out.println(tr.size());
		File dir = new File(urlsDir);
		// for (int i = 0; i < NUM_THREADS; i++)
		// graphs.add(new FollowingGraph(usersDirectory, MAX_FOLLOWERS_CACHED
		// / NUM_THREADS));
		File[] list = dir.listFiles();
		System.out.println("Total Files: " + list.length);
		ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS);
		for (File f : list) {
			if (finishedFiles.containsKey(f.getName()))
				continue;
			Task task = new Task(f);
			executor.execute(task);
			// if (cnt == 5000)
			// break;
		}
		executor.shutdown();
		executor.awaitTermination(60, TimeUnit.MINUTES);
		System.out.println(tr.size());
		UsersReader reader = UsersReader.getInstance();
		FileWriter fw = new FileWriter(outDirectory + "TotalData.txt", false);
		for (Entry<Long, UserNode> e : tr.entrySet()) {
			UserData u = reader.getUser(e.getKey());
			if (u != null && e.getValue().minLocalInf != Integer.MAX_VALUE) {
				StringBuilder sb = new StringBuilder();
				sb.append(u.followers + ",");
				sb.append(u.friends + ",");
				sb.append(e.getValue().tweets + ",");
				sb.append(u.dateJoined + ",");
				sb.append(e.getValue().totalInfSum.floatValue()
						/ e.getValue().tweets.floatValue() + ",");
				sb.append(e.getValue().minTotalInf + ",");
				sb.append(e.getValue().maxTotalInf + ",");

				sb.append(e.getValue().localInfSum.floatValue()
						/ e.getValue().tweets.floatValue() + ",");
				sb.append(e.getValue().minLocalInf + ",");
				sb.append(e.getValue().maxLocalInf + "\n");
				fw.write(sb.toString());
			}
		}
		fw.close();
	}

	private static void writeHashMap(String directory, Object map)
			throws IOException {
		FileOutputStream fout = new FileOutputStream(directory);
		ObjectOutputStream oos = new ObjectOutputStream(fout);
		oos.writeObject(map);

		oos.close();
		fout.close();
	}

	private static ConcurrentHashMap<String, String> readHashMap(
			String directory) throws IOException, ClassNotFoundException {
		FileInputStream fin;
		try {
			fin = new FileInputStream(directory);
		} catch (FileNotFoundException e) {
			writeHashMap(directory, new ConcurrentHashMap<String, String>());
			fin = new FileInputStream(directory);
		}

		ObjectInputStream ois = new ObjectInputStream(fin);
		@SuppressWarnings("unchecked")
		ConcurrentHashMap<String, String> map = (ConcurrentHashMap<String, String>) ois
				.readObject();
		ois.close();

		return map;
	}

	private static ConcurrentHashMap<Long, UserNode> readUserMap(
			String directory) throws IOException, ClassNotFoundException {
		FileInputStream fin;
		try {
			fin = new FileInputStream(directory);
		} catch (FileNotFoundException e) {
			writeHashMap(directory, new ConcurrentHashMap<Long, UserNode>());
			fin = new FileInputStream(directory);
		}

		ObjectInputStream ois = new ObjectInputStream(fin);
		@SuppressWarnings("unchecked")
		ConcurrentHashMap<Long, UserNode> map = (ConcurrentHashMap<Long, UserNode>) ois
				.readObject();
		ois.close();

		return map;
	}

	private static Semaphore s1 = new Semaphore(1);
	private static Semaphore s2 = new Semaphore(1);
	private static Semaphore s3 = new Semaphore(1);
	private static Semaphore s4 = new Semaphore(1);

	static class UserNode implements Serializable {

		public UserNode() {
			tweets = new AtomicInteger(0);
			totalInfSum = new AtomicInteger(0);
			localInfSum = new AtomicInteger(0);
		}

		// total number of tweets ( different urls posted )
		AtomicInteger tweets;
		// average total is totalInfSum/tweets
		AtomicInteger totalInfSum;
		// average local is localInfSum/tweets
		AtomicInteger localInfSum;

		int minTotalInf = Integer.MAX_VALUE;
		int minLocalInf = Integer.MAX_VALUE;

		int maxTotalInf = Integer.MIN_VALUE;
		int maxLocalInf = Integer.MIN_VALUE;

		public void updateMinTotalInf(int x) throws InterruptedException {
			s1.acquire();
			if (minTotalInf > x)
				minTotalInf = x;
			s1.release();
		}

		public void updateMaxTotalInf(int x) throws InterruptedException {
			s2.acquire();
			if (maxTotalInf < x)
				maxTotalInf = x;
			s2.release();
		}

		public void updateMinLocalInf(int x) throws InterruptedException {
			s3.acquire();
			if (minLocalInf > x)
				minLocalInf = x;
			s3.release();
		}

		public void updateMaxLocalInf(int x) throws InterruptedException {
			s4.acquire();
			if (maxLocalInf < x)
				maxLocalInf = x;
			s4.release();
		}
	}

	static class Task implements Runnable {
		File f;

		// make this smaller when the date is removed from the URLs files
		private static int MAX_FILE_SIZE = 50000;

		// Graph graph;

		public Task(File f) {
			this.f = f;
		}

		public ArrayList<String> readData(DataInputStream dis)
				throws IOException {
			byte[] bytes = new byte[MAX_FILE_SIZE];
			int size = dis.available(), loaded = dis.read(bytes);
			if (size != loaded && loaded > 0) {
				// file is too big to be processed
				return null;
			}
			ByteArrayInputStream in = new ByteArrayInputStream(bytes, 0, size);
			BufferedReader buff = new BufferedReader(new InputStreamReader(in));
			ArrayList<String> lines = new ArrayList<String>();
			while (true) {
				String s = buff.readLine();
				if (s == null)
					break;
				lines.add(s);
			}
			return lines;
		}

		@Override
		public void run() {
			DataInputStream dis = null;
			try {
				// graph = graphs.poll();
				dis = new DataInputStream(new FileInputStream(f));
				ArrayList<String> lines = readData(dis);
				if (lines == null)
					return;
				if (lines.size() > 1500) {
					System.out.println("Ignoring file " + f.getName()
							+ " with size " + lines.size());
					return;
				}
				HashSet<Long> completedUsers = new HashSet<Long>();
				TreeMap<Long, Integer> local = new TreeMap<Long, Integer>();
				TreeMap<Long, Integer> total = new TreeMap<Long, Integer>();
				TreeMap<Long, Long> parent = new TreeMap<Long, Long>();
				for (int i = 0; i < lines.size(); i++) {
					StringTokenizer st = new StringTokenizer(lines.get(i), ":");
					long fromId = Long.parseLong(st.nextToken());
					if (!graph.exists(fromId))
						continue;

					if (!completedUsers.contains(fromId)) {
						if (!tr.containsKey(fromId))
							tr.put(fromId, new UserNode());
						tr.get(fromId).tweets.incrementAndGet();
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
					long parentId;
					if (parent.containsKey(child))
						parentId = parent.get(child);
					else
						parentId = child;
					if (!total.containsKey(parentId))
						total.put(parentId, 0);
					if (local.containsKey(child)) // zero if no cascades from
													// this child
						total.put(parentId,
								total.get(parentId) + local.get(child));
				}
				for (Entry<Long, Integer> e : local.entrySet()) {
					if (!tr.containsKey(e.getKey()))
						tr.put(e.getKey(), new UserNode());
					UserNode u = tr.get(e.getKey());
					u.localInfSum.addAndGet(e.getValue());
					if (!total.containsKey(e.getKey()))
						total.put(e.getKey(), 0);
					total.put(e.getKey(), total.get(e.getKey()) + e.getValue());
					u.updateMinLocalInf(e.getValue());
					u.updateMaxLocalInf(e.getValue());
				}
				for (Entry<Long, Integer> e : total.entrySet()) {
					if (!tr.containsKey(e.getKey()))
						tr.put(e.getKey(), new UserNode());
					UserNode u = tr.get(e.getKey());
					if (!e.getValue().equals(new Integer(0))) {
						u.totalInfSum.addAndGet(e.getValue());
						u.updateMinTotalInf(e.getValue());

						// System.out.println("updating Mx total Info "
						// + e.getValue());
						u.updateMaxTotalInf(e.getValue());
						// System.out.println("finished with Mx total info "
						// + e.getValue());
					}
				}
				int d = done.incrementAndGet();
				finishedFiles.put(f.getName(), "");
				if (d % 1000 == 0) {
					System.out.println("Finished Files: " + done);
					System.out.println("Total users in tree " + tr.size());
				}
				if (d % 10000 == 0) {
					writeHashMap("finishedUsers", tr);
					writeHashMap("finishedFiles", finishedFiles);
				}
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				// if (graph != null)
				// graphs.add(graph);

				try {
					dis.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

	}
}
