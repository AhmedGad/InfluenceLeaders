import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Queue;
import java.util.TreeSet;

import twitter4j.IDs;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationBuilder;

public class Main_Graph_v2 {
	private static Configuration getConfiguration(String ConsumerKey,
			String ConsumerSecret, String AccessToken, String TokenSecret) {
		ConfigurationBuilder cb = new ConfigurationBuilder();
		cb.setDebugEnabled(true).setOAuthConsumerKey(ConsumerKey)
				.setOAuthConsumerSecret(ConsumerSecret)
				.setOAuthAccessToken(AccessToken)
				.setOAuthAccessTokenSecret(TokenSecret);
		return cb.build();
	}

	private static Twitter getTwitterInstance(Configuration conf) {
		return new TwitterFactory(conf).getInstance();
	}

	// to be computed on runtime
	static int totalTokens = 0, flushLimit = 1000;
	static Queue<Integer> freeTokens = new ArrayDeque<Integer>();

	static String accesstokens[][];

	static Connection con;
	static Statement st;

	static Queue<Long> usersQueue;
	static Queue<Long> unfinished_queue;
	static HashMap<Long, UserEntry> unfinished_map;

	static TreeSet<Long> userId;

	static int max_batch_size = 100000;
	static int database_cnt = 0;
	private static int collected_cnt = 0;
	private static int total_fetched = 0;

	static Queue<ArrayList<Long>> graphQueue;
	private static int threadNum = 30;

	static synchronized long pop() {
		// check unfinished first
		if (!unfinished_queue.isEmpty())
			return unfinished_queue.poll();

		if (usersQueue.isEmpty())
			return -1;
		return usersQueue.poll();
	}

	static synchronized void add_unfinished_user_entry(long uid, long cursor,
			ArrayList<Long> followers) {
		unfinished_queue.add(uid);
		unfinished_map.put(uid, new UserEntry(cursor, followers));

	}

	public static void fetch(int tokenIndex, int threadIndex)
			throws TwitterException, Exception {

		Twitter twitter = getTwitterInstance(getConfiguration(
				accesstokens[tokenIndex][0], accesstokens[tokenIndex][1],
				accesstokens[tokenIndex][2], accesstokens[tokenIndex][3]));

		long curUser = -1, curCursor = -1;
		ArrayList<Long> followers = null;
		boolean userOk = true;

		while (true) {
			try {
				// pick user from the queue
				if ((curUser = pop()) > -1) {
					if (unfinished_map.containsKey((Long) curUser)) {
						UserEntry entry = unfinished_map.remove((Long) curUser);
						curCursor = entry.cursor;
						followers = entry.followers;
					}
					userOk = false;
					followers = new ArrayList<Long>();
					// user with index = 0 is the followed user
					followers.add(curUser);
					while (true) {
						IDs res = twitter.getFollowersIDs(curUser, curCursor);
						long[] followers_ar = res.getIDs();
						curCursor = res.getNextCursor();
						for (int i = 0; i < followers_ar.length; i++)
							followers.add(followers_ar[i]);

						total_fetched += followers_ar.length;

						if (!res.hasNext()) {
							graphQueue.add(followers);
							collected_cnt += followers.size() - 1;
							userOk = true;
							curCursor = -1;
							break;
						}
					}
				} else {
					// if users queue is empty - sleep 1 sec
					Thread.sleep(1000);
				}
			} catch (Exception e) {
				if (e instanceof TwitterException) {
					if (((TwitterException) e).getLocalizedMessage()
							.startsWith("401:Authentication credentials")) {
						userOk = true;
						st.execute("delete from queue where id = " + curUser);
					}
				}
				throw e;
			} finally {
				if (!userOk) {
					add_unfinished_user_entry(curUser, curCursor, followers);
				}
			}
		}
	}

	@SuppressWarnings("resource")
	public static void writer() throws SQLException, InterruptedException,
			IOException {
		FileWriter logWriter = new FileWriter(new File(("log"
				+ System.currentTimeMillis() + ".txt")));
		PreparedStatement userid_insertion = con
				.prepareStatement("insert into userid values(?)");

		PreparedStatement graph_insertion = con
				.prepareStatement("insert into graph values(?, ?)");

		PreparedStatement queue_insertion = con
				.prepareStatement("insert into queue values(?)");

		// I think better commint if windows
		con.setAutoCommit(false);

		ArrayList<Long> cur = null;

		while (true) {
			try {
				if (!graphQueue.isEmpty()) {
					synchronized (graphQueue) {
						cur = graphQueue.poll();
					}
					// System.out.println("===\t try to write " + cur.size()
					// + " record.");
					// followed user
					Long uid1, uid2 = cur.get(0);
					int last = 0;
					long t1 = System.currentTimeMillis();
					for (int i = 1; i < cur.size(); i++) {
						uid1 = cur.get(i);
						graph_insertion.setLong(1, uid1);
						graph_insertion.setLong(2, uid2);
						graph_insertion.addBatch();

						if (!userId.contains(uid1)) {
							userid_insertion.setLong(1, uid1);
							userid_insertion.addBatch();
							queue_insertion.setLong(1, uid1);
							queue_insertion.addBatch();

							usersQueue.add(uid1);
							userId.add(uid1);
						}

						if ((i % max_batch_size) == 0 || i == cur.size() - 1) {
							userid_insertion.executeBatch();
							queue_insertion.executeBatch();
							graph_insertion.executeBatch();

							con.commit();

							database_cnt += i - last;
							last = i;
						}
					}

					st.execute("delete from queue where id = " + uid2);
					System.out.println("finish writing " + (cur.size() - 1)
							+ " records in "
							+ (System.currentTimeMillis() - t1)
							+ " millis, waited list " + graphQueue.size()
							+ " user id " + uid2);
					logWriter.write("finish writing " + (cur.size() - 1)
							+ " records in "
							+ (System.currentTimeMillis() - t1)
							+ " millis, waited list " + graphQueue.size()
							+ " user id " + uid2 + "\n");
					logWriter.flush();
				} else {
					Thread.sleep(1000);
				}
			} catch (Exception e) {
				e.printStackTrace();
				synchronized (graphQueue) {
					graphQueue.add(cur);
				}
			}
		}
	}

	@SuppressWarnings("resource")
	static void printCnt() throws IOException {
		FileWriter writer = new FileWriter(new File("counters"
				+ System.currentTimeMillis() + ".txt"));
		int min = 0;
		while (true) {
			System.out.println("\n\nmin: " + min++ + " written to database, "
					+ database_cnt + " fetched, " + collected_cnt + " gap, "
					+ (collected_cnt - database_cnt) + " databas queue size : "
					+ graphQueue.size() + " total fetched: " + total_fetched
					+ "\n\n");

			writer.write("min: " + min++ + " written to database, "
					+ database_cnt + " fetched, " + collected_cnt + " gap, "
					+ (collected_cnt - database_cnt) + " databas queue size : "
					+ graphQueue.size() + " total fetched: " + total_fetched
					+ "\n");
			writer.flush();
			try {
				Thread.sleep(60000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	private static void init() throws SQLException, Exception {
		graphQueue = new ArrayDeque<>();
		usersQueue = new ArrayDeque<Long>();
		unfinished_queue = new ArrayDeque<Long>();
		unfinished_map = new HashMap<Long, UserEntry>();
		userId = new TreeSet<Long>();

		ResultSet res = st.executeQuery("select * from userid;");
		while (res.next()) {
			userId.add(res.getLong(1));
		}

		ArrayList<Long> list = new ArrayList<Long>();
		
		res = st.executeQuery("select * from queue;");
		while (res.next()) {
			list.add(res.getLong(1));
		}
		Collections.shuffle(list);
		for (int i = 0; i < list.size(); i++) {
			usersQueue.add(list.get(i));
		}
		list.clear();
		// ============================================
		BufferedReader tokens = new BufferedReader(new FileReader(new File(
				"tokens.txt")));

		while (tokens.ready()) {
			tokens.readLine();
			totalTokens++;
		}

		tokens.close();

		totalTokens /= 5;
		accesstokens = new String[totalTokens][4];

		System.out.println("total tokens: " + totalTokens);

		tokens = new BufferedReader(new FileReader(new File("tokens.txt")));
		for (int i = 0; i < accesstokens.length; i++) {
			for (int j = 0; j < accesstokens[i].length; j++)
				accesstokens[i][j] = tokens.readLine();
			tokens.readLine();
		}
	}

	public static void main(String[] args) throws Exception {
		con = Database.getConnection();
		st = con.createStatement();

		init();

		Thread printerThread = new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					printCnt();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		});
		printerThread.start();

		Thread writerThread = new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					writer();
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		});
		writerThread.start();

		int curTokenNumber = 0;
		for (int i = 0; i < threadNum; i++) {
			new Thread(new MyThread(curTokenNumber++, i)).start();
		}

		for (int i = curTokenNumber; i < totalTokens; i++) {
			freeTokens.add(i);
		}
	}

	static class UserEntry {
		long cursor;
		ArrayList<Long> followers;

		public UserEntry(long cursor, ArrayList<Long> followers) {
			this.cursor = cursor;
			this.followers = followers;
		}
	}

	static class MyThread implements Runnable {
		int tokenIndex, threadIndex;

		public MyThread(int tokenIndex, int threadIndex) {
			this.tokenIndex = tokenIndex;
			this.threadIndex = threadIndex;
		}

		@Override
		public void run() {
			while (true) {
				try {
					Main_Graph_v2.fetch(tokenIndex, threadIndex);
				} catch (TwitterException e) {
					// sleep 2 sec on exception
					try {
						Thread.sleep(2000);
					} catch (InterruptedException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}

					if (e.getErrorMessage() != null
							&& e.getErrorMessage()
									.equals("Rate limit exceeded")) {
						synchronized (freeTokens) {
							freeTokens.add(tokenIndex);
							tokenIndex = freeTokens.poll();
						}
					}
				} catch (Exception e) {
				}
			}
		}
	}
}
