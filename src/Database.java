import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;

public class Database {

	static Connection con;

	static Connection getConnection() throws Exception {
		if (con == null) {
			String unicode = "?useServerPrepStmts=false&rewriteBatchedStatements=true";
			String url = "jdbc:mysql://localhost:3306/twitterdb";
			String user = "root";
			String password = "root";
			con = DriverManager.getConnection(url + unicode, user, password);
		}
		return con;
	}

	public static void main(String[] args) throws Exception {
		Connection con = getConnection();
		Statement st = con.createStatement();
		boolean ok = false;
		int start = 0, step = 5000000;
		long t1 = System.currentTimeMillis();
		while (!ok) {
			st.executeQuery("select * from userid limit " + start + "," + step
					+ ";");
			start += step;
			System.out.println("curr: " + start);
			if (start == 10000000) {
				System.out.println((System.currentTimeMillis() - t1) / 1000);
				break;
			}
		}
	}
}
