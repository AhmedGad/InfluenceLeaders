import java.net.*;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.io.*;

public class htmlPage {
	public static void main(String[] args) throws Exception {
		String ret = "data-activity-popup-title=\"Retweeted";
		String fav = "data-activity-popup-title=\"Favorited";

		Connection c = Database.getConnection();
		Statement st = c.createStatement();
		ResultSet res = st.executeQuery("select t_id from tweet;");
		int cnt = 0;
		while (res.next()) {

			System.out.println(++cnt);

			res.next();
			long id = new Long(res.getString(1));
			URL TweetURL = new URL("https://mobile.twitter.com/twitter/status/"
					+ id);
			URLConnection yc = TweetURL.openConnection();
			BufferedReader in = new BufferedReader(new InputStreamReader(
					yc.getInputStream()));
			String inputLine;

			while ((inputLine = in.readLine()) != null) {
				if (inputLine.contains(ret) || inputLine.contains(fav))
					System.out.println(inputLine + "\t" + id);
			}
			in.close();
		}
	}
}
