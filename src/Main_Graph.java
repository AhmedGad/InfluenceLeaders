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
import java.util.ArrayList;
import java.util.Arrays;

import twitter4j.IDs;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationBuilder;


public class Main_Graph {
	// to be computed on runtime
	static int totalTokens = 0, flushLimit = 1000;

	static String accesstokens[][];

	// connection to dataBase
	static Connection con;
	static long totalEdges = 0;
	static PreparedStatement pstmt_Queue, pstmt_Graph, pstmt_UserId;
	static Statement globalStmt;
	static ArrayList<Long> tmpList;

	static String firstElementQuery = "select * from Queue order by ID asc limit 1;";

	private static Configuration getConfiguration(String ConsumerKey, String ConsumerSecret,
			String AccessToken, String TokenSecret) {
		ConfigurationBuilder cb = new ConfigurationBuilder();
		cb.setDebugEnabled(true).setOAuthConsumerKey(ConsumerKey)
				.setOAuthConsumerSecret(ConsumerSecret).setOAuthAccessToken(AccessToken)
				.setOAuthAccessTokenSecret(TokenSecret);
		return cb.build();
	}

	private static Configuration getConfiguration() {
		return getConfiguration("sxEreYQOtd3d93PxZpm90Q",
				"jxYJyZIMUP1hv6hDqpbFD6jXA6PZcKDQYD3E1o82zsA",
				"280089745-FuYA5sJhdKndOUNxb8HxUzEQwiUQSuEMeltHmKt9",
				"30HLHlx4C0cFEHMGjHUvK5oXKFTtVZ7mTplZsbsDhBLrX");
	}

	private static Twitter getTwitterInstance(Configuration conf) {
		return new TwitterFactory(conf).getInstance();
	}

	private static Twitter getTwitterInstance() {
		return getTwitterInstance(getConfiguration());
	}

	/**
	 * 
	 * @param tokenIndex
	 *            index of token
	 * @param userId
	 *            user ID
	 * @param cursor
	 *            cursor for next group of followers
	 * @return returns -1 if this user has finished his followers <br>
	 *         returns nextCursor if current token has ended
	 * @throws TwitterException
	 */
	private static long run(int tokenIndex, long userId, long cursor) throws TwitterException {
		Twitter twitter = getTwitterInstance(getConfiguration(accesstokens[tokenIndex][0],
				accesstokens[tokenIndex][1], accesstokens[tokenIndex][2],
				accesstokens[tokenIndex][3]));

		Arrays.binarySearch(a, key);
		System.out.println("Token : " + tokenIndex + ", User ID : " + userId + ", Cursor : "
				+ cursor);
		IDs list = null;
		try {
			do {
				list = twitter.getFollowersIDs(userId, cursor);
				long[] ids = list.getIDs();
				for (int i = 0; i < ids.length; i++)
					if (!isVisited(ids[i])) {
						// add element to list
						tmpList.add(ids[i]);
					}

				totalEdges += ids.length;
				cursor = list.getNextCursor();
			} while (list.hasNext());
		} catch (Exception e) {
			e.printStackTrace();

			// token finished return next cursor
			return cursor;
		}

		// current user finished
		return -1;
	}

	private static void printCnt() throws IOException {
		@SuppressWarnings("resource")
		FileWriter writer = new FileWriter(new File("tweets-per-minute"
				+ System.currentTimeMillis() + ".txt"));
		int minutes = 1;
		long prevEdges = totalEdges;
		while (true) {
			try {
				Thread.sleep(60000);
				System.out.println("minute \t" + minutes + "\tTotal Edges:\t"
						+ (totalEdges - prevEdges) + "\n");

				writer.write("minute \t" + minutes + ":\tTotal Edges:\t" + (totalEdges - prevEdges)
						+ "\n");

				prevEdges = totalEdges;

				minutes++;
				writer.flush();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	private static boolean isVisited(long e) throws SQLException {
		ResultSet res = globalStmt.executeQuery("select * from UserId where ID = " + e + ";");
		return res.first();
	}

	private static long getQueueElement() throws SQLException {
		ResultSet res = globalStmt.executeQuery(firstElementQuery);
		if (res.first())
			return Long.parseLong(res.getString(1));
		return -1;
	}

	private static void removeQueueElement(long u) throws SQLException {
		globalStmt.execute("delete from Queue where id = " + u + ";");
	}

	public static void main(String[] args) throws Exception {
		BufferedReader tokens = new BufferedReader(new FileReader(new File("tokens.txt")));

		while (tokens.ready()) {
			tokens.readLine();
			totalTokens++;
		}

		System.out.println(totalTokens);
		totalTokens /= 5;

		tokens.close();
		tokens = new BufferedReader(new FileReader(new File("tokens.txt")));

		System.out.println(totalTokens);
		accesstokens = new String[totalTokens][4];
		for (int i = 0; i < accesstokens.length; i++) {
			for (int j = 0; j < accesstokens[i].length; j++) {
				accesstokens[i][j] = tokens.readLine();
			}
			tokens.readLine();
		}

		// printer thread
		Thread printerThread = new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					printCnt();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		});
		printerThread.start();

		// initialize database statments
		con = Database.getConnection();

		String sql = "INSERT INTO UserId (id) VALUES(?)";
		pstmt_UserId = con.prepareStatement(sql);

		String sq2 = "INSERT INTO Queue (id) VALUES(?)";
		pstmt_Queue = con.prepareStatement(sq2);

		String sq3 = "INSERT INTO Graph (id1, id2) VALUES(?, ?)";
		pstmt_Graph = con.prepareStatement(sq3);

		globalStmt = con.createStatement();

		tmpList = new ArrayList<Long>();

		// Main part
		int currToken = 0;

		while (true) {
			try {
				tmpList.clear();

				// get element from queue
				long u = getQueueElement();

				// process that element
				long cursor = -1;
				do {
					long nc = run(currToken, u, cursor);

					if (cursor != -1 || nc == cursor)
						currToken = (currToken + 1) % totalTokens;

					cursor = nc;
				} while (cursor != -1);

				// flush data into the database
				for (int i = 0; i < tmpList.size(); i++) {
					long k = tmpList.get(i);

					pstmt_UserId.setLong(1, k);
					pstmt_UserId.addBatch();

					pstmt_Queue.setLong(1, k);
					pstmt_Queue.addBatch();

					pstmt_Graph.setLong(1, u);
					pstmt_Graph.setLong(2, k);
					pstmt_Graph.addBatch();

					if (i > 0 && i % flushLimit == 0) {
						// execute commands
						pstmt_UserId.executeBatch();
						pstmt_Queue.executeBatch();
						pstmt_Graph.executeBatch();
					}

				}

				

				// remove that element from Queue
				removeQueueElement(u);

				// update total Sum
				totalEdges += tmpList.size();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

	}
}
