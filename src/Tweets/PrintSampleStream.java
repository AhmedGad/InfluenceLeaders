package Tweets;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectOutputStream;

import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterException;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationBuilder;

/**
 * get Stream from twitter<br>
 * can get stream or track some words
 */
public final class PrintSampleStream {

	static Object lock = new Object();
	static int TweetCount = 0;
	static FileWriter writer;

	private static Configuration getConfiguration(String ConsumerKey,
			String ConsumerSecret, String AccessToken, String TokenSecret) {
		ConfigurationBuilder cb = new ConfigurationBuilder();
		cb.setDebugEnabled(true).setOAuthConsumerKey(ConsumerKey)
				.setOAuthConsumerSecret(ConsumerSecret)
				.setOAuthAccessToken(AccessToken)
				.setOAuthAccessTokenSecret(TokenSecret);
		return cb.build();
	}

	private static Configuration getConfiguration() {
		return getConfiguration("LnI1jQwh1e4zHskx1rWcPA",
				"W1Z5nK1vTniWfgJH6VrTYMYtGHsxSQdx9tNOBrC77o",
				"810448838-aFk8dsqX477Nx59eduknXPbm42VkieD6Y1yRLs0i",
				"ZoBYRhYoBlWrVP8KPpkFlPbrALlOEgTasjwnRkEBXjZWK");
	}

	private static void printCnt() throws IOException {
		FileWriter writer = new FileWriter(new File("tweets-per-minute"
				+ System.currentTimeMillis() + ".txt"));
		int prev = 0;
		int minutes = 1;
		while (true) {
			try {
				Thread.sleep(60000);
				System.out.println("Minute: " + minutes + ", Tweets: "
						+ (TweetCount - prev) + ", Averge: "
						+ (TweetCount / minutes) + ", Total: " + TweetCount
						+ "\n");

				writer.write("Minute: " + minutes + ", Tweets: "
						+ (TweetCount - prev) + ", Averge: "
						+ (TweetCount / minutes) + ", Total: " + TweetCount
						+ "\n");
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

		// BufferedReader trackingWords = new BufferedReader(new FileReader(
		// "TrackingWords.txt"));
		// int size = Integer.parseInt(trackingWords.readLine());
		// for (int i = 0; i < size; i++)
		// tWords[i] = trackingWords.readLine();

		String[] tWords = { "ly", "tinyurl", "gl" };

		fq.track(tWords);
		// fq.language(new String[] { "en" });

		twitterStream.filter(fq);
	}

	private void sample(TwitterStream twitterStream) {
		twitterStream.sample();
	}

	public static void main(String[] args) throws TwitterException, IOException {
		final TwitterStream twitterStream = new TwitterStreamFactory(
				getConfiguration()).getInstance();

		StatusListener listener = new StatusListener() {
			FileOutputStream fout = new FileOutputStream(
					"./Status/status @ "
							+ System.currentTimeMillis() + ".txt");

			ObjectOutputStream oos = new ObjectOutputStream(fout);

			@Override
			public void onStatus(Status status) {
				synchronized (lock) {
					TweetCount++;
				}
				try {
					oos.writeObject(status);
					if (TweetCount % 1000 == 0) {
						oos.flush();
						oos.reset();
					}
					if (TweetCount % 100000 == 0) {
						oos.close();
						fout.close();

						fout = new FileOutputStream(
								"./Status/status @ "
										+ System.currentTimeMillis() + ".txt");
						oos = new ObjectOutputStream(fout);
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
			}

			@Override
			public void onDeletionNotice(
					StatusDeletionNotice statusDeletionNotice) {
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
		// print.sample(twitterStream);
		print.filter(twitterStream);
	}
}