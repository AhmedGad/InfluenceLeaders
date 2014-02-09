import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

import twitter4j.*;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationBuilder;

public final class PrintSampleStream {

	static Object lock = new Object();
	static int TweetCount = 0;
	static FileWriter writer;

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

	private static void printCnt() throws IOException {
		FileWriter writer = new FileWriter(new File("tweets-per-minute"
				+ System.currentTimeMillis() + ".txt"));
		int prev = 0;
		int minutes = 1;
		while (true) {
			try {
				Thread.sleep(60000);
				System.out.println("Minute: " + minutes + ", Tweets: " + (TweetCount - prev)
						+ ", Averge: " + (TweetCount / minutes) + ", Total: " + TweetCount + "\n");

				writer.write("Minute: " + minutes + ", Tweets: " + (TweetCount - prev)
						+ ", Averge: " + (TweetCount / minutes) + ", Total: " + TweetCount + "\n");
				minutes++;
				writer.flush();
				prev = TweetCount;
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	private void filter(TwitterStream twitterStream) throws IOException {
		FilterQuery fq = new FilterQuery();

		// get any tweet with geo-tagging enabled
		// fq.locations(new double[][] { { -180, -90 }, { 180, 90 } });

		BufferedReader trackingWords = new BufferedReader(new FileReader("TrackingWords.txt"));
		int size = Integer.parseInt(trackingWords.readLine());
		String[] tWords = new String[size];
		for (int i = 0; i < size; i++)
			tWords[i] = trackingWords.readLine();

		fq.track(tWords);
		fq.language(new String[] { "en" });

		twitterStream.filter(fq);
	}

	private void sample(TwitterStream twitterStream) {
		twitterStream.sample();
	}

	public static void main(String[] args) throws TwitterException, IOException {
		final TwitterStream twitterStream = new TwitterStreamFactory(getConfiguration())
				.getInstance();

		final BufferedWriter statusWriter = new BufferedWriter(new FileWriter("status @ "
				+ System.currentTimeMillis() + ".txt"));
		StatusListener listener = new StatusListener() {
			@Override
			public void onStatus(Status status) {
				synchronized (lock) {
					TweetCount++;
				}
				StringBuilder sb = new StringBuilder();
				sb.append("UserName : " + status.getUser().getScreenName());
				sb.append("\nCreation Time : " + status.getCreatedAt());
				sb.append("\nPlace : " + status.getPlace());
				sb.append("\nStatus : " + status.getText());
				sb.append("\n=====================\n");

				try {
					statusWriter.write(sb.toString());
					if (TweetCount % 1000 == 0)
						statusWriter.flush();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}

			@Override
			public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
			}

			@Override
			public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
				// System.out.println("Got track limitation notice:"
				// + numberOfLimitedStatuses);
			}

			@Override
			public void onScrubGeo(long userId, long upToStatusId) {
				// System.out.println("Got scrub_geo event userId:" + userId
				// + " upToStatusId:" + upToStatusId);
			}

			@Override
			public void onStallWarning(StallWarning warning) {
				// System.out.println("Got stall warning:" + warning);
				// twitterStream.shutdown();
				// twitterStream.sample();
			}

			@Override
			public void onException(Exception ex) {
				// System.out.println("got exception");
				// ex.printStackTrace();
				// twitterStream.shutdown();
				// twitterStream.sample();
			}
		};
		twitterStream.addListener(listener);
		PrintSampleStream print = new PrintSampleStream();

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

		// Either sample or filter can run within the same stream instance
		// print.filter(twitterStream);
		// print.sample(twitterStream);

		print.filter(twitterStream);
	}
}