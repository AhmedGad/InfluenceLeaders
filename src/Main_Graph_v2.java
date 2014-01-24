import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayDeque;
import java.util.ArrayList;
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

	static synchronized long pop() {
		// check unfinished first
		if (!unfinished_queue.isEmpty())
			return unfinished_queue.poll();

		if (usersQueue.isEmpty())
			return -1;
		return usersQueue.poll();
	}

	static Queue<ArrayList<Long>> graphQueue;
	private static int threadNum = 30;

	static synchronized void push(long uid) throws SQLException {
		usersQueue.add(uid);
		st.execute("insert into userid value(" + uid + ");");
		st.execute("insert into queue value(" + uid + ");");
	}

	static synchronized void add_unfinished_user_entry(long uid, long cursor,
			ArrayList<Long> followers) {

		unfinished_queue.add(uid);

		UserEntry entry = unfinished_map.get(uid);
		if (entry == null) {
			unfinished_map.put(uid, new UserEntry(cursor, followers));
		} else {
			entry.cursor = cursor;
			entry.followers = followers;
		}
	}

	public static void writer() throws SQLException, InterruptedException {

		PreparedStatement userid_insertion = con
				.prepareStatement("insert into userid value(?)");

		PreparedStatement graph_insertion = con
				.prepareStatement("insert into graph value(?, ?);");

		PreparedStatement queue_insertion = con
				.prepareStatement("insert into queue value(?)");

		// I think better commint if windows
		con.setAutoCommit(false);

		ArrayList<Long> cur = null;

		while (true) {
			if (graphQueue.size() > 0)
				System.out.println(graphQueue.size());
			try {
				if (!graphQueue.isEmpty()) {
					synchronized (graphQueue) {
						cur = graphQueue.poll();
					}
					System.out.println("===\t try to write " + cur.size()
							+ " record.");
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
					System.out.println("finish writing " + cur.size()
							+ " records in "
							+ (System.currentTimeMillis() - t1));
				}
			} catch (Exception e) {
				e.printStackTrace();
				synchronized (graphQueue) {
					graphQueue.add(cur);
				}
			}
		}
	}

	public static void run(int tokenIndex, int threadIndex)
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

						collected_cnt += followers_ar.length;
						if (!res.hasNext()) {
							graphQueue.add(followers);
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

		res = st.executeQuery("select * from queue;");
		while (res.next()) {
			usersQueue.add(res.getLong(1));
		}

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

	static void printCnt() {
		while (true) {
			System.out.println(database_cnt + " " + collected_cnt);
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public static void main(String[] args) throws Exception {
		con = Database.getConnection();
		st = con.createStatement();

		init();

		Thread printerThread = new Thread(new Runnable() {
			@Override
			public void run() {
				printCnt();
			}
		});
		printerThread.start();

		Thread writerThread = new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					writer();
				} catch (SQLException | InterruptedException e) {
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
					Main_Graph_v2.run(tokenIndex, threadIndex);
				} catch (TwitterException e) {

					System.out.println("\t\t\t\t\t=======================\t"
							+ "tokenIndex: " + tokenIndex + "\tthreadIndex: "
							+ threadIndex + "\t" + e.getErrorMessage());

					if (e.getErrorMessage() == null) {
						System.out.println("===\t" + e.getLocalizedMessage());
					}
					// e.printStackTrace();
					if (e.getErrorMessage() != null
							&& e.getErrorMessage()
									.equals("Rate limit exceeded")) {
						freeTokens.add(tokenIndex);
						tokenIndex = freeTokens.poll();
					}
				} catch (Exception e) {
				}
			}
		}
	}
}
