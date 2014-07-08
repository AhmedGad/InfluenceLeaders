package dataPreparation;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.HashSet;
import java.util.StringTokenizer;

public class CalcGraphSize {

	private byte[] bytes = new byte[800000000];
	private final static int READ_FOLLOWERS = 0;
	private final static int NEW_USER = 1;
	private final static int FIRST_LINE_AFTER_NEW_USER = 2;
	private final String graphDir = "../../data/Graph/";
	private HashSet<Long> set;

	public CalcGraphSize(File activeUsers) throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader(activeUsers));
		String s;
		while ((s = reader.readLine()) != null) {
			set.add(Long.parseLong(s));
		}
		reader.close();
		System.out.println("active users: " + set.size());
		log.println("active users: " + set.size());
		log.flush();
	}

	PrintWriter log = new PrintWriter(new File("log-GraphIndexing.txt"));
	long OriginalEdgesCnt, TrimmedEdgesCnt, nodesCnt;

	public void calc() throws Exception {
		File inputFile = new File(graphDir);
		String s;
		long t1 = System.currentTimeMillis();

		PrintWriter errorLog = new PrintWriter(new File(
				"errorlog-GraphIndexing.txt"));
		int NumUsers = 0, NumUsers2 = 0;

		for (File f : inputFile.listFiles()) {
			if (f.getName().startsWith("graph") && f.getName().endsWith(".txt")) {
				log.println(f.getAbsolutePath());

				int state = NEW_USER;

				FileInputStream fis;
				DataInputStream dis = new DataInputStream(
						fis = new FileInputStream(f));
				int size = dis.available(), loaded = dis.read(bytes);
				if (size != loaded && loaded > 0) {
					System.err.println(f.getAbsolutePath() + " loaded: "
							+ loaded + " total Size: " + size);
					errorLog.println(f.getAbsolutePath() + " loaded: " + loaded
							+ " total Size: " + size);
					errorLog.flush();
				}

				ByteArrayInputStream in = new ByteArrayInputStream(bytes, 0,
						size);
				BufferedReader buff = new BufferedReader(new InputStreamReader(
						in));

				while (true) {
					s = buff.readLine();
					if (s == null) {
						break;
					}
					if (state == READ_FOLLOWERS) {
						if (s.length() == 0) {
							state++;
						} else {
							try {
								long uid = Long.parseLong(s);
								OriginalEdgesCnt++;
								if (set.contains(uid)) {
									TrimmedEdgesCnt++;
								}
							} catch (Exception e) {
							}
						}
					} else if (state == NEW_USER) {
						NumUsers++;
						NumUsers2++;
						StringTokenizer tok = new StringTokenizer(s);

						tok.nextToken();

						if (!tok.nextToken().equals("followers:")) {
							System.err.println(f.getAbsolutePath()
									+ "\t\"followers:\" word not found!!"
									+ "\tcur line: " + s);
							errorLog.write(f.getAbsolutePath() + "\t"
									+ "\"followers:\" word not found!!"
									+ "\tcur line: " + s + "\n");
						}
						state++;
					} else if (state == FIRST_LINE_AFTER_NEW_USER) {
						if (s.length() > 0) {
							System.err
									.println(f.getAbsolutePath()
											+ "\tfirst line is not empty!!!!\tcur line: "
											+ s);
							errorLog.write(f.getAbsolutePath()
									+ "\tfirst line is not empty!!!!\tcur line: "
									+ s + "\n");
						}
						state = 0;
					}
				}

				fis.close();
				dis.close();
				if (NumUsers2 > 1000) {
					long elapsed = (System.currentTimeMillis() - t1) / 1000;
					System.out.println("nodes: " + NumUsers
							+ " original graph edge counter: "
							+ OriginalEdgesCnt + " trimmed graph edge cnt: "
							+ TrimmedEdgesCnt + ", elapsed time: " + elapsed
							+ "seconds, users/sec: " + NumUsers / elapsed
							+ ", trimmed/original graph size: "
							+ (TrimmedEdgesCnt * 100 / OriginalEdgesCnt)
							/ 100.0);
					log.println("\nnodes: " + NumUsers
							+ " original graph edge counter: "
							+ OriginalEdgesCnt + " trimmed graph edge cnt: "
							+ TrimmedEdgesCnt + ", elapsed time: " + elapsed
							+ "seconds, users/sec: " + NumUsers / elapsed
							+ ", trimmed/original graph size: "
							+ (TrimmedEdgesCnt * 100 / OriginalEdgesCnt)
							/ 100.0 + "\n");
					log.flush();
					NumUsers2 = 0;
				}
			}
		}
		System.out.println("nodes: " + NumUsers
				+ " original graph edge counter: " + OriginalEdgesCnt
				+ " trimmed graph edge cnt: " + TrimmedEdgesCnt
				+ ", time millis: " + (System.currentTimeMillis() - t1));
		log.println("nodes: " + NumUsers + ", original graph edge counter: "
				+ OriginalEdgesCnt + ", trimmed graph edge cnt: "
				+ TrimmedEdgesCnt + ", time millis: "
				+ (System.currentTimeMillis() - t1));
		log.flush();
		errorLog.close();
		System.out.println("Finished");
	}

	public static void main(String[] args) throws Exception {
		File activeUsers = new File("activeUser.txt");
		if (!activeUsers.exists()) {
			System.err.println("File " + activeUsers.getAbsolutePath()
					+ " does not exist!");
			System.exit(0);
		}
		CalcGraphSize graphCalculator = new CalcGraphSize(activeUsers);
		graphCalculator.calc();
	}
}