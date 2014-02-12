import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
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
	static Queue<Integer> freeTokens2 = new ArrayDeque<Integer>();

	static String accesstokens[][];

	// user id not finished yet , not written to database, still in user queue
	static Queue<Long> unfinished_queue;
	static HashMap<Long, UserEntry> unfinished_map;

	static Queue<Long> unfinished_queue2;
	static HashMap<Long, UserEntry> unfinished_map2;

	// ready to be written to graph [user .... followers]
	static Queue<MyArrayList> graphQueue;

	static int max_batch_size = 100000;

	static int database_cnt = 0;
	private static int collected_cnt = 0;
	private static int collected_cnt2 = 0;

	private static int total_fetched = 0;
	private static int total_fetched2 = 0;

	private static int threadNum = 60;

	private static BufferedReader queueReader;
	private static BufferedReader queueReader2;

	/**
	 * get new user id from queue
	 * 
	 * @return
	 * @throws IOException
	 */
	static synchronized long pop(boolean fetchFollowers) throws IOException {
		String id = null;

		if (fetchFollowers) {
			// check unfinished first
			if (!unfinished_queue.isEmpty())
				return unfinished_queue.poll();

			id = queueReader.readLine();
		} else {
			// check unfinished first
			if (!unfinished_queue2.isEmpty())
				return unfinished_queue2.poll();

			id = queueReader2.readLine();
		}

		if (id == null)
			return -1;

		return new Long(id);
	}

	static synchronized void add_unfinished_user_entry(long uid, long cursor,
			MyArrayList followers, boolean fetchFollowers) {
		if (fetchFollowers) {
			unfinished_queue.add(uid);
			unfinished_map.put(uid, new UserEntry(cursor, followers));
		} else {
			unfinished_queue2.add(uid);
			unfinished_map2.put(uid, new UserEntry(cursor, followers));
		}
	}

	static synchronized UserEntry getEntry(long curUser, boolean fetchFollowers) {
		UserEntry entry = fetchFollowers ? unfinished_map
				.remove((Long) curUser) : unfinished_map2
				.remove((Long) curUser);
		if (entry == null) {
			MyArrayList list = new MyArrayList(fetchFollowers);
			entry = new UserEntry(-1, list);
			list.add(curUser);
		}
		return entry;
	}

	static Object lock1 = new Object();
	static Object lock2 = new Object();

	static String filesTimeStamp = System.currentTimeMillis() + "";
	static FileWriter finishedWriter;
	static FileWriter finished2Writer;
	static FileWriter graphWriter;

	/**
	 * fetch followers
	 * 
	 * @param tokenIndex
	 * @throws TwitterException
	 * @throws Exception
	 */
	public static void fetch(int tokenIndex, boolean fetchFollowers)
			throws TwitterException, Exception {

		Twitter twitter = getTwitterInstance(getConfiguration(
				accesstokens[tokenIndex][0], accesstokens[tokenIndex][1],
				accesstokens[tokenIndex][2], accesstokens[tokenIndex][3]));

		long curUser = -1, curCursor = -1;
		MyArrayList followers = null;
		boolean userFinished = true;

		while (true) {
			try {
				// pick user from the queue
				if ((curUser = pop(fetchFollowers)) > -1) {
					UserEntry entry = getEntry(curUser, fetchFollowers);
					curCursor = entry.cursor;
					followers = entry.followers;

					userFinished = false;
					while (true) {
						IDs res = null;
						if (fetchFollowers)
							res = twitter.getFollowersIDs(curUser, curCursor);
						else
							res = twitter.getFriendsIDs(curUser, curCursor);

						long[] followers_ar = res.getIDs();
						curCursor = res.getNextCursor();
						for (int i = 0; i < followers_ar.length; i++)
							followers.add(followers_ar[i]);

						if (fetchFollowers)
							total_fetched += followers_ar.length;
						else
							total_fetched2 += followers_ar.length;

						if (!res.hasNext()) {
							synchronized (graphQueue) {
								graphQueue.add(followers);
							}
							synchronized (lock2) {
								if (fetchFollowers)
									collected_cnt += followers.size() - 1;
								else
									collected_cnt2 += followers.size() - 1;
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
					if (fetchFollowers) {
						finishedWriter.write(curUser + "\n");
						finishedWriter.flush();
					} else {
						finished2Writer.write(curUser + "\n");
						finished2Writer.flush();
					}
				}
				synchronized (errorLog) {
					try {
						errorLog.write("token: " + tokenIndex
								+ "\tfetch followers: " + fetchFollowers
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
					add_unfinished_user_entry(curUser, curCursor, followers,
							fetchFollowers);
				}
			}
		}
	}

	static int finished1Cnt = 0;
	static int finished2Cnt = 0;

	@SuppressWarnings("resource")
	public static void writer() throws InterruptedException, IOException {
		FileWriter logWriter = new FileWriter(new File(("log"
				+ System.currentTimeMillis() + ".txt")));

		MyArrayList cur = null;
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
					int last = 0;
					long t1 = System.currentTimeMillis();
					for (int i = 1; i < cur.size(); i++) {
						uid1 = cur.get(i);
						if (cur.isFollowers) {
							graphWriter.write(uid1 + " " + uid2 + "\n");
						} else {
							graphWriter.write(uid2 + " " + uid1 + "\n");
						}

						if ((i % max_batch_size) == 0 || i == cur.size() - 1) {
							graphWriter.flush();

							database_cnt += i - last;
							reopenCnt += i - last;
							last = i;
						}
					}

					if (reopenCnt > 500000) {
						reopenCnt = 0;
						graphWriter.close();
						graphWriter = new FileWriter(new File("graph @ "
								+ System.currentTimeMillis() + ".txt"));
					}

					if (cur.isFollowers) {
						finished1Cnt++;
						synchronized (finishedWriter) {
							finishedWriter.write(uid2 + "\n");
							finishedWriter.flush();
						}
					} else {
						finished2Cnt++;
						synchronized (finished2Writer) {
							finished2Writer.write(uid2 + "\n");
							finished2Writer.flush();
						}
					}

					if (graphQueue.size() == 0
							&& collected_cnt + collected_cnt2 - database_cnt != 0)
						System.out
								.println("\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n========================\n\n\n\n\n\n\n\n\n\n\n\n");

					System.out.println((cur.isFollowers ? "" : "\t\t")
							+ "finish writing " + (cur.size() - 1)
							+ " records in "
							+ (System.currentTimeMillis() - t1)
							+ " millis, waited list " + graphQueue.size()
							+ " user id " + uid2 + " finished1: "
							+ finished1Cnt + " finished2: " + finished2Cnt);

					logWriter.write((cur.isFollowers ? "" : "\t\t")
							+ "finish writing " + (cur.size() - 1)
							+ " records in "
							+ (System.currentTimeMillis() - t1)
							+ " millis, waited list " + graphQueue.size()
							+ " user id " + uid2 + " finished1: "
							+ finished1Cnt + " finished2: " + finished2Cnt
							+ "\n");

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
					+ database_cnt + " ,fetched1: " + collected_cnt
					+ " ,fetched2: " + collected_cnt2 + " ,gap: "
					+ (collected_cnt + collected_cnt2 - database_cnt)
					+ " ,databas queue size : " + graphQueue.size()
					+ " ,total fetched1: " + total_fetched
					+ " ,total fetched2: " + total_fetched2 + " ,sum: "
					+ (total_fetched + total_fetched2) + "\n\n");

			writer.write("min: " + min++ + " written to database, "
					+ database_cnt + " ,fetched1: " + collected_cnt
					+ " ,fetched2: " + collected_cnt2 + " ,gap: "
					+ (collected_cnt + collected_cnt2 - database_cnt)
					+ " ,databas queue size : " + graphQueue.size()
					+ " ,total fetched1: " + total_fetched
					+ " ,total fetched2: " + total_fetched2 + " ,sum: "
					+ (total_fetched + total_fetched2) + "\n");

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
		graphQueue = new ArrayDeque<MyArrayList>();

		unfinished_queue = new ArrayDeque<Long>();
		unfinished_map = new HashMap<Long, UserEntry>();

		unfinished_queue2 = new ArrayDeque<Long>();
		unfinished_map2 = new HashMap<Long, UserEntry>();

		finishedWriter = new FileWriter("finished @ " + filesTimeStamp
				+ " .txt");
		finished2Writer = new FileWriter("2finished @ " + filesTimeStamp
				+ ".txt");
		graphWriter = new FileWriter("graph @ " + filesTimeStamp + ".txt");

		System.out.println("Start");

		HashSet<Long> finished = new HashSet<Long>();
		HashSet<Long> finished2 = new HashSet<Long>();

		loadFinished(finished, finished2);

		System.out.println("Finish Loading finish table");

		FileWriter usersQueue = new FileWriter("queue.txt");
		FileWriter usersQueue2 = new FileWriter("queue2.txt");

		loadQueues(usersQueue, usersQueue2, finished, finished2);

		usersQueue.close();
		usersQueue2.close();

		queueReader = new BufferedReader(new FileReader(new File("queue.txt")));
		queueReader2 = new BufferedReader(
				new FileReader(new File("queue2.txt")));

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

	private static void loadQueues(FileWriter usersQueue,
			FileWriter usersQueue2, HashSet<Long> finished,
			HashSet<Long> finished2) throws Exception {

		ArrayList<File> idList = FileGetter.getListForPrefix("userID");
		int cnt1 = 0;
		int cnt2 = 0;

		for (File file : idList) {
			BufferedReader buff = new BufferedReader(new FileReader(file));
			String id;
			while ((id = buff.readLine()) != null) {
				Long curId = new Long(id);
				if (!finished.contains(curId)) {
					cnt1++;
					usersQueue.write(id + "\n");
					finished.add(curId);
				}
				if (!finished2.contains(curId)) {
					cnt2++;
					usersQueue2.write(id + "\n");
					finished2.add(curId);
				}
				if (cnt1 >= 1000000 && cnt2 >= 1000000)
					break;
			}
			buff.close();
			if (cnt1 >= 1000000 && cnt2 >= 1000000)
				break;
		}
	}

	private static void loadFinished(HashSet<Long> finished,
			HashSet<Long> finished2) throws Exception {
		ArrayList<File> finishedList = FileGetter.getListForPrefix("finished");
		int cnt = 0;
		for (File file : finishedList) {
			BufferedReader buff = new BufferedReader(new FileReader(file));
			String id;
			while ((id = buff.readLine()) != null) {
				finished.add(new Long(id));
				cnt++;
			}
			buff.close();
		}

		System.out.println(cnt + " finished user loaded.");
		cnt = 0;

		ArrayList<File> finished2List = FileGetter
				.getListForPrefix("2finished");
		for (File file : finished2List) {
			BufferedReader buff = new BufferedReader(new FileReader(file));
			String id;
			while ((id = buff.readLine()) != null) {
				finished2.add(new Long(id));
				cnt++;
			}
			buff.close();
		}
		System.out.println(cnt + " finished2 user loaded.");
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
			new Thread(new MyThread(curTokenNumber, i, i % 2 == 0)).start();
			if (i % 2 == 1)
				curTokenNumber++;
		}

		for (int i = curTokenNumber; i < totalTokens; i++) {
			freeTokens.add(i);
			freeTokens2.add(i);
		}
	}

	static class MyArrayList {
		boolean isFollowers;

		private ArrayList<long[]> list;
		private int listIndx, arrIndx;
		private final int len = 5000;

		public MyArrayList(boolean followers) {
			list = new ArrayList<long[]>();
			list.add(new long[len]);
			listIndx = 0;
			arrIndx = 0;
			isFollowers = followers;
		}

		void add(long n) {
			if (arrIndx == len) {
				arrIndx = 0;
				listIndx++;
				list.add(new long[len]);
			}
			list.get(listIndx)[arrIndx] = n;
			arrIndx++;
		}

		long get(int i) {
			return list.get(i / len)[i % len];
		}

		int size() {
			return listIndx * len + arrIndx;
		}
	}

	static class UserEntry {
		long cursor;
		MyArrayList followers;

		public UserEntry(long cursor, MyArrayList followers) {
			this.cursor = cursor;
			this.followers = followers;
		}
	}

	static class MyThread implements Runnable {
		int tokenIndex, threadIndex;
		boolean fetchFollowers;

		public MyThread(int tokenIndex, int threadIndex, boolean fetchFollowers) {
			this.tokenIndex = tokenIndex;
			this.threadIndex = threadIndex;
			this.fetchFollowers = fetchFollowers;
		}

		@Override
		public void run() {
			while (true) {
				try {
					Main_Graph_v2.fetch(tokenIndex, fetchFollowers);
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
						if (fetchFollowers)
							synchronized (freeTokens) {
								freeTokens.add(tokenIndex);
								tokenIndex = freeTokens.poll();
							}
						else
							synchronized (freeTokens2) {
								freeTokens2.add(tokenIndex);
								tokenIndex = freeTokens2.poll();
							}
					}
				} catch (Exception e) {
				}
			}
		}
	}
}
