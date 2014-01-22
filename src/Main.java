//import java.io.BufferedReader;
//import java.io.File;
//import java.io.FileReader;
//import java.io.FileWriter;
//import java.io.IOException;
//import java.util.ArrayList;
//
//import twitter4j.Twitter;
//import twitter4j.TwitterException;
//import twitter4j.TwitterFactory;
//import twitter4j.conf.Configuration;
//import twitter4j.conf.ConfigurationBuilder;
//
//public class Main {
//
//	public static Configuration getConfiguration(String ConsumerKey, String ConsumerSecret,
//			String AccessToken, String TokenSecret) {
//		ConfigurationBuilder cb = new ConfigurationBuilder();
//		cb.setDebugEnabled(true).setOAuthConsumerKey(ConsumerKey)
//				.setOAuthConsumerSecret(ConsumerSecret).setOAuthAccessToken(AccessToken)
//				.setOAuthAccessTokenSecret(TokenSecret);
//		return cb.build();
//	}
//
//	public static Configuration getConfiguration() {
//		return getConfiguration("sxEreYQOtd3d93PxZpm90Q",
//				"jxYJyZIMUP1hv6hDqpbFD6jXA6PZcKDQYD3E1o82zsA",
//				"280089745-FuYA5sJhdKndOUNxb8HxUzEQwiUQSuEMeltHmKt9",
//				"30HLHlx4C0cFEHMGjHUvK5oXKFTtVZ7mTplZsbsDhBLrX");
//	}
//
//	public static Twitter getTwitterInstance(Configuration conf) {
//		return new TwitterFactory(conf).getInstance();
//	}
//
//	public static Twitter getTwitterInstance() {
//		return getTwitterInstance(getConfiguration());
//	}
//
//	static void run(int tokenIndex, int threadIndex) throws TwitterException {
//		Twitter twitter = getTwitterInstance(getConfiguration(accesstokens[tokenIndex][0],
//				accesstokens[tokenIndex][1], accesstokens[tokenIndex][2],
//				accesstokens[tokenIndex][3]));
//		for (int i = 0; i < 200; i++) {
//			twitter.showStatus(tweets[(int) (Math.random() * tweetsNum)]);
//			cnt[threadIndex]++;
//		}
//	}
//
//	static int threadNum = 30;
//	static int tokenNumber = 0;
//	// to be computed on runtime
//	static int totalTokens = 0;
//	// to be computed on runtime
//	static int tweetsNum = 0;
//	static String accesstokens[][];
//	static long tweets[];
//	static int cnt[] = new int[threadNum];
//	static Object lock = new Object();
//
//	static void printCnt() throws IOException {
//		@SuppressWarnings("resource")
//		FileWriter writer = new FileWriter(new File("tweets-per-minute"
//				+ System.currentTimeMillis() + ".txt"));
//		int sum = 0;
//		int prev = sum;
//		int minutes = 1;
//		while (true) {
//			try {
//				Thread.sleep(60000);
//				sum = 0;
//				for (int i = 0; i < cnt.length; i++) {
//					sum += cnt[i];
//				}
//				System.out.println("minute \t" + minutes + ":\ttweets:\t" + (sum - prev)
//						+ "\tavg:\t" + (sum / minutes) + "\ttotal\t" + sum);
//				writer.write(("minute \t" + minutes + ":\ttweets:\t" + (sum - prev) + "\tavg:\t"
//						+ (sum / minutes) + "\ttotal\t" + sum + "\n"));
//				minutes++;
//				writer.flush();
//				prev = sum;
//			} catch (InterruptedException e) {
//				e.printStackTrace();
//			}
//		}
//	}
//
//	public static void main(String[] args) throws Exception {
//		BufferedReader tokens = new BufferedReader(new FileReader(new File("tokens.txt")));
//		BufferedReader tweets = new BufferedReader(new FileReader(new File("tweets")));
//
//		while (tokens.ready()) {
//			tokens.readLine();
//			totalTokens++;
//		}
//		System.out.println(totalTokens);
//		totalTokens /= 5;
//
//		while (tweets.ready()) {
//			tweets.readLine();
//			tweetsNum++;
//		}
//
//		Main.tweets = new long[tweetsNum];
//		tweets.close();
//		tokens.close();
//		
//		tokens = new BufferedReader(new FileReader(new File("tokens.txt")));
//		tweets = new BufferedReader(new FileReader(new File("tweets")));
//
//		System.out.println(totalTokens);
//		System.out.println(tweetsNum);
//		accesstokens = new String[totalTokens][4];
//		for (int i = 0; i < accesstokens.length; i++) {
//			for (int j = 0; j < accesstokens[i].length; j++)
//				accesstokens[i][j] = tokens.readLine();
//			tokens.readLine();
//		}
//
//		for (int i = 0; i < tweetsNum; i++) {
//			Main.tweets[i] = new Long(tweets.readLine());
//		}
//
//		Thread printerThread = new Thread(new Runnable() {
//			@Override
//			public void run() {
//				try {
//					printCnt();
//				} catch (IOException e) {
//					// TODO Auto-generated catch block
//					e.printStackTrace();
//				}
//			}
//		});
//		printerThread.start();
//
//		ArrayList<Thread> list = new ArrayList<Thread>();
//		ArrayList<myclass> runnableList = new ArrayList<myclass>();
//		for (int i = 0; i < threadNum; i++) {
//			runnableList.add(new myclass(tokenNumber++, i));
//			Thread t = new Thread(runnableList.get(runnableList.size() - 1));
//			list.add(t);
//			t.start();
//		}
//
//		while (true) {
//			for (int i = 0; i < list.size(); i++) {
//				try {
//					list.get(i).join();
//					tokenNumber++;
//					tokenNumber %= totalTokens;
//					runnableList.get(i).tokenIndex = tokenNumber;
//					list.remove(i);
//					list.add(i, new Thread(runnableList.get(i)));
//					list.get(i).start();
//				} catch (Exception e) {
//					e.printStackTrace();
//				}
//			}
//		}
//	}
//
//	static class myclass implements Runnable {
//		int tokenIndex, threadIndex;
//
//		public myclass(int tokenIndex, int threadIndex) {
//			this.tokenIndex = tokenIndex;
//			this.threadIndex = threadIndex;
//		}
//
//		@Override
//		public void run() {
//			while (true) {
//				try {
//					Main.run(tokenIndex, threadIndex);
//				} catch (TwitterException e) {
//					System.out.println("\t\t\t\t\t=======================\t" + tokenIndex + "\t"
//							+ threadIndex + "\t" + e.getErrorMessage());
//					if (e.getErrorMessage() == null)
//						e.printStackTrace();
//					if (e.getErrorMessage() != null
//							&& e.getErrorMessage().equals("Rate limit exceeded")) {
//						break;
//					}
//				}
//			}
//		}
//	}
//}
