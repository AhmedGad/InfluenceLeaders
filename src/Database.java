import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class Database {

	static Connection con;

	static Connection getConnection() throws Exception {
		if (con == null) {
			String unicode = "?useUnicode=yes&characterEncoding=UTF-8";
			String url = "jdbc:mysql://localhost:3306/twitter";
			String user = "root";
			String password = "root";
			con = DriverManager.getConnection(url + unicode, user, password);
		}
		return con;
	}

	public static void main(String[] args) throws Exception {
		Connection con = getConnection();
		Statement st = con.createStatement();
		ResultSet res = st.executeQuery("select * from tweet;");
		while (res.next()) {
			if (res.getString(1) != null) {
				System.out.println(res.getString(1) + "\n");
			}
		}
	}
}
