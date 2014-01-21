import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import twitter4j.*;

public final class PrintSampleStream {

	static Object lock = new Object();
	static int cnt = 0;
	static int cnt1 = 0, cnt2 = 0;
	static long t1 = System.currentTimeMillis();
	static FileWriter writer;

	void filter(TwitterStream twitterStream) {
		FilterQuery fq = new FilterQuery();

		// get any tweet with geo-tagging enabled
		// fq.locations(new double[][] { { -180, -90 }, { 180, 90 } });

		fq.track(new String[] { "a", "an", "the", ".", "so", ",", "he", "she",
				"it", "I", "is", "are", "am", "'m" });
		fq.language(new String[] { "en" });

		twitterStream.filter(fq);
	}

	void sample(TwitterStream twitterStream) {
		twitterStream.sample();
	}

	public static void main(String[] args) throws TwitterException, IOException {
		final TwitterStream twitterStream = new TwitterStreamFactory(
				Main.getConfiguration()).getInstance();
		StatusListener listener = new StatusListener() {

			@Override
			public void onStatus(Status status) {
				synchronized (lock) {
					cnt++;
				}
				// System.out.println(status.getUser().getScreenName() + " "
				// + status.getCreatedAt() + " " + status.getPlace() + " "
				// + status.getText());
				// System.out.println(status.getId());
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
		// Either sample or filter can run within the same stream instance

		// print.filter(twitterStream);
		print.sample(twitterStream);
		int prev = cnt;
		while (true) {
			// System.out.println(cnt - prev);
			prev = cnt;
			try {
				Thread.sleep(10000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}
}