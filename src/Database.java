import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

public class Database {

	static Connection con;

	static Connection getConnection() throws Exception {
		if (con == null) {
			String unicode = "?useServerPrepStmts=false&rewriteBatchedStatements=true";
			String url = "jdbc:mysql://localhost:3306/mydb";
			String user = "root";
			String password = "root";
			con = DriverManager.getConnection(url + unicode, user, password);
		}
		return con;
	}

	public static void main(String[] args) throws Exception {
		Connection con = getConnection();
		con.setAutoCommit(false);
		PreparedStatement st = con
				.prepareStatement("insert into userid value(?)");
		long t1 = System.currentTimeMillis();
		for (int i = 0; i < 1000; i++) {
			st.setLong(1, (long) (Math.random() * 1000000000000000000L));
			st.addBatch();
		}
		System.out.println(System.currentTimeMillis() - t1);
		t1 = System.currentTimeMillis();
		st.executeBatch();
		System.out.println(System.currentTimeMillis() - t1);
		con.commit();
	}
}
