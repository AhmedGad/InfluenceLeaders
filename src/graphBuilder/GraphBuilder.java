package graphBuilder;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Queue;

import twitter4j.IDs;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationBuilder;

public class GraphBuilder {
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

	// user id not finished yet , not written to database, still in user queue
	static Queue<Long> unfinished_queue;
	static HashMap<Long, UserEntry> unfinished_map;

	// ready to be written to graph [user .... followers]
	static Queue<MyLongArrayList> graphQueue;

	static int max_batch_size = 100000;

	static int database_cnt = 0;
	private static int collected_cnt = 0;

	private static int total_fetched = 0;
	private static int total_finished = 0;

	private static int threadNum = 60;

	private static BufferedReader queueReader;

	/**
	 * get new user id from queue
	 * 
	 * @return
	 * @throws IOException
	 */
	static synchronized long pop() throws IOException {
		String id = null;

		// check unfinished first
		if (!unfinished_queue.isEmpty())
			return unfinished_queue.poll();

		id = queueReader.readLine();

		if (id == null)
			return -1;

		return new Long(id);
	}

	static synchronized void add_unfinished_user_entry(long uid, long cursor,
			MyLongArrayList followers) {
		unfinished_queue.add(uid);
		unfinished_map.put(uid, new UserEntry(cursor, followers));
	}

	static synchronized UserEntry getEntry(long curUser) {
		UserEntry entry = unfinished_map.remove((Long) curUser);
		if (entry == null) {
			MyLongArrayList list = new MyLongArrayList();
			entry = new UserEntry(-1, list);
			list.add(curUser);
		}
		return entry;
	}

	static Object lock1 = new Object();
	static Object lock2 = new Object();

	static String filesTimeStamp = System.currentTimeMillis() + "";
	static FileWriter finishedWriter;
	static FileWriter graphWriter;

	/**
	 * fetch followers
	 * 
	 * @param tokenIndex
	 * @throws TwitterException
	 * @throws Exception
	 */
	public static void fetch(int tokenIndex) throws TwitterException, Exception {

		Twitter twitter = getTwitterInstance(getConfiguration(
				accesstokens[tokenIndex][0], accesstokens[tokenIndex][1],
				accesstokens[tokenIndex][2], accesstokens[tokenIndex][3]));

		long curUser = -1, curCursor = -1;
		MyLongArrayList followers = null;
		boolean userFinished = true;

		while (true) {
			try {
				// pick user from the queue
				if ((curUser = pop()) > -1) {
					UserEntry entry = getEntry(curUser);
					curCursor = entry.cursor;
					followers = entry.followers;

					userFinished = false;
					while (true) {
						IDs res = null;
						res = twitter.getFollowersIDs(curUser, curCursor);

						long[] followers_ar = res.getIDs();
						curCursor = res.getNextCursor();
						for (int i = 0; i < followers_ar.length; i++)
							followers.add(followers_ar[i]);
						synchronized (lock1) {
							total_fetched += followers_ar.length;
						}
						if (!res.hasNext()) {
							synchronized (graphQueue) {
								graphQueue.add(followers);
							}
							synchronized (lock2) {
								collected_cnt += followers.size() - 1;
							}

							userFinished = true;
							curCursor = -1;
							break;
						}
					}
				} else {
					// if users queue is empty - sleep 1 sec
					Thread.sleep(1000);
				}
			} catch (TwitterException e) {
				if (e.getLocalizedMessage().startsWith(
						"401:Authentication credentials")
						|| e.getLocalizedMessage().startsWith(
								"404:The URI requested is invalid")) {
					userFinished = true;
					finishedWriter.write(curUser + "\n");
					total_finished++;
					finishedWriter.flush();
				}
				synchronized (errorLog) {
					try {
						errorLog.write("token: " + tokenIndex
								+ "\terror message: " + e.getErrorMessage()
								+ "\tlocalized message: "
								+ e.getLocalizedMessage() + "\tuser id: "
								+ curUser + "\tcursor: " + curCursor
								+ "\n==============================\n\n");
						errorLog.flush();
					} catch (IOException e1) {
					}
				}
				throw e;
			} finally {
				if (!userFinished) {
					add_unfinished_user_entry(curUser, curCursor, followers);
				}
			}
		}
	}

	static int finished1Cnt = 0;

	@SuppressWarnings("resource")
	public static void writer() throws InterruptedException, IOException {
		FileWriter logWriter = new FileWriter(new File(("log"
				+ System.currentTimeMillis() + ".txt")));

		MyLongArrayList cur = null;
		int reopenCnt = 0;

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
					graphWriter.write(uid2 + " followers:\n\n");

					int last = 0;
					long t1 = System.currentTimeMillis();
					for (int i = 1; i < cur.size(); i++) {
						uid1 = cur.get(i);
						graphWriter.write(uid1 + "\n");

						if ((i % max_batch_size) == 0 || i == cur.size() - 1) {
							graphWriter.flush();

							database_cnt += i - last;
							reopenCnt += i - last;
							last = i;
						}
					}
					graphWriter.write("\n");

					if (reopenCnt > 10000000) {
						reopenCnt = 0;
						graphWriter.close();
						graphWriter = new FileWriter(new File("graph @ "
								+ System.currentTimeMillis() + ".txt"));
					}

					finished1Cnt++;
					synchronized (finishedWriter) {
						finishedWriter.write(uid2 + "\n");
						total_finished++;
						finishedWriter.flush();
					}

					if (graphQueue.size() == 0
							&& collected_cnt - database_cnt != 0)
						System.out
								.println("\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n========================\n\n\n\n\n\n\n\n\n\n\n\n");

					System.out.println("finish writing " + (cur.size() - 1)
							+ " records in "
							+ (System.currentTimeMillis() - t1)
							+ " millis, waited list " + graphQueue.size()
							+ " user id " + uid2 + " finished1: "
							+ finished1Cnt);

					logWriter.write("finish writing " + (cur.size() - 1)
							+ " records in "
							+ (System.currentTimeMillis() - t1)
							+ " millis, waited list " + graphQueue.size()
							+ " user id " + uid2 + " finished1: "
							+ finished1Cnt + "\n");

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

			System.out.println("\n\nmin: " + min + " written to database, "
					+ database_cnt + " ,fetched: " + collected_cnt + " ,gap: "
					+ (collected_cnt - database_cnt)
					+ " ,databas queue size : " + graphQueue.size()
					+ " ,total fetched: " + total_fetched + " total users: "
					+ total_finished + "\n\n");

			writer.write("min: " + min++ + " written to database, "
					+ database_cnt + " ,fetched: " + collected_cnt + " ,gap: "
					+ (collected_cnt - database_cnt)
					+ " ,databas queue size : " + graphQueue.size()
					+ " ,total fetched: " + total_fetched + " total users: "
					+ total_finished + "\n");

			writer.flush();
			try {
				Thread.sleep(60000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	private static void init() throws Exception {
		graphQueue = new ArrayDeque<MyLongArrayList>();

		unfinished_queue = new ArrayDeque<Long>();
		unfinished_map = new HashMap<Long, UserEntry>();

		System.out.println("Start");

		HashSet<Long> finished = new HashSet<Long>();

		loadFinished(finished);

		System.out.println("Finish Loading finish table");

		finishedWriter = new FileWriter("finished @ " + filesTimeStamp
				+ " .txt");
		graphWriter = new FileWriter("graph @ " + filesTimeStamp + ".txt");

		FileWriter usersQueue = new FileWriter("queue.txt");

		loadQueues(usersQueue, finished);

		usersQueue.close();

		queueReader = new BufferedReader(new FileReader(new File("queue.txt")));

		System.out.println("Finished Loading ALL");
		finished.clear();
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

	private static void loadQueues(FileWriter usersQueue, HashSet<Long> vis)
			throws Exception {
		System.out.println("\nInitiate queue: ");
		ArrayList<File> idList = FileGetter.getListForPrefix("userID", true);
		int cnt = 0;
		float sum = 0;
		for (File file : idList) {
			BufferedReader buff = new BufferedReader(new FileReader(file));
			String id;
			int unique = 0, total = 0;
			while ((id = buff.readLine()) != null) {
				total++;
				sum++;
				Long curId = new Long(id);
				if (!vis.contains(curId)) {
					cnt++;
					unique++;
					usersQueue.write(id + "\n");
					vis.add(curId);
				}
				if (cnt >= 1000000)
					break;
			}
			System.out.println(file.getName() + " total: " + total + " new: "
					+ unique + " percentage: " + (unique * 100 / total) + "%");
			buff.close();
			if (cnt >= 1000000)
				break;
		}
		System.out.println(cnt + " New users added to queue."
				+ " New percentage: " + cnt * 100 / sum + "%");
	}

	private static void loadFinished(HashSet<Long> finished) throws Exception {
		ArrayList<File> finishedList = FileGetter.getListForPrefix("finished",
				true);
		int cnt = 0;
		for (File file : finishedList) {
			BufferedReader buff = new BufferedReader(new FileReader(file));
			String id;
			int i = 0;
			while ((id = buff.readLine()) != null) {
				finished.add(new Long(id));
				cnt++;
				i++;
			}
			System.out.println(file.getName() + " " + i);
			buff.close();
		}

		System.out.println(cnt + " finished user loaded.");
	}

	static FileWriter errorLog;

	public static void main(String[] args) throws Exception {
		init();

		errorLog = new FileWriter("errorLog.txt");

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
			new Thread(new MyThread(curTokenNumber, i)).start();
			if (i % 2 == 1)
				curTokenNumber++;
		}

		for (int i = curTokenNumber; i < totalTokens; i++) {
			freeTokens.add(i);
		}
	}

	static class UserEntry {
		long cursor;
		MyLongArrayList followers;

		public UserEntry(long cursor, MyLongArrayList followers) {
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
					GraphBuilder.fetch(tokenIndex);
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
