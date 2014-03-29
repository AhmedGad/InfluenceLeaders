package dataPreparation;

import graph.UserFollowers;
import graphBuilder.MyIntegerArrayList;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.StringTokenizer;
import java.util.TreeMap;

/**
 * Given the Graph files this class will extract every user as a file that
 * contains all followers for this user<br>
 * the input for this class is : <br>
 * <ul>
 * <li>graphDir : the directory of graph files.</li>
 * <li>indexedDir : the directory of the output files, the files that will
 * contain every user's follower list.</li>
 * <li>setDir : directory of the HashSet that contains graph files names,
 * because in case the class stop don't begin the whole process again just begin
 * from the last file, it can be any directory <br>
 * the HashSet file will be created after the first run</li>
 * </ul>
 */
public class GraphIndexing {

	private byte[] bytes = new byte[800000000];
	private final static int READ_FOLLOWERS = 0;
	private final static int NEW_USER = 1;
	private final static int FIRST_LINE_AFTER_NEW_USER = 2;
	private final String outDir = "./Users-trimmed6/";
	private final String graphDir = "./Graph/";
	private HashSet<Long> set;
	private final HashMap<Long, Integer> map;
	private static int nextId = 0;

	public GraphIndexing(File activeUsers) throws IOException {
		set = new HashSet<Long>();
		map = new HashMap<Long, Integer>();
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
	long beforeCnt, afteCnt;

	public TreeMap<Integer, UserFollowers> buildMap() throws Exception {
		TreeMap<Integer, UserFollowers> graphMap = new TreeMap<Integer, UserFollowers>();

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

				MyIntegerArrayList list = null;
				int id = -1;

				while (true) {
					s = buff.readLine();
					if (s == null) {
						if (list != null) {
							graphMap.put(id, new UserFollowers(id, list));
						}
						break;
					}
					if (state == READ_FOLLOWERS) {
						if (s.length() == 0) {
							graphMap.put(id, new UserFollowers(id, list));
							state++;
						} else {
							try {
								long uid = Long.parseLong(s);
								beforeCnt++;
								if (set.contains(uid)) {
									afteCnt++;
									if (!map.containsKey(uid))
										map.put(uid, nextId++);
									list.add(map.get(uid));
								}
							} catch (Exception e) {
							}
						}
					} else if (state == NEW_USER) {
						NumUsers++;
						NumUsers2++;
						StringTokenizer tok = new StringTokenizer(s);

						long uid = Long.parseLong(tok.nextToken());
						if (!map.containsKey(uid))
							map.put(uid, nextId++);
						id = map.get(uid);
						list = new MyIntegerArrayList();

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
					System.out.println("finished users: " + NumUsers
							+ " before cnt: " + beforeCnt + " after cnt: "
							+ afteCnt + ", elapsed time: " + elapsed
							+ "seconds, users/sec: " + NumUsers / elapsed
							+ ", trimmed/original graph size: "
							+ (afteCnt * 100 / beforeCnt) / 100.0);
					log.println("\nfinished users: " + NumUsers
							+ " before cnt: " + beforeCnt + " after cnt: "
							+ afteCnt + ", elapsed time: " + elapsed
							+ "seconds, users/sec: " + NumUsers / elapsed
							+ ", trimmed/original graph size: "
							+ (afteCnt * 100 / beforeCnt) / 100.0 + "\n");
					log.flush();
					NumUsers2 = 0;
				}
			}
		}
		System.out.println("finished users: " + NumUsers + " before cnt: "
				+ beforeCnt + " after cnt: " + afteCnt + ", time millis: "
				+ (System.currentTimeMillis() - t1));
		log.println("finished users: " + NumUsers + ", before cnt: "
				+ beforeCnt + ", after cnt: " + afteCnt + ", time millis: "
				+ (System.currentTimeMillis() - t1));
		log.flush();
		errorLog.close();
		System.out.println("Finished");
		writeHashMap("UsersMapping", map);
		return graphMap;
	}

	private static void writeHashMap(String directory, Object map)
			throws IOException {
		FileOutputStream fout = new FileOutputStream(directory);
		ObjectOutputStream oos = new ObjectOutputStream(fout);
		oos.writeObject(map);

		oos.close();
		fout.close();
	}

	public static void main(String[] args) throws Exception {
		File activeUsers = new File("activeUser.txt");
		if (!activeUsers.exists()) {
			System.exit(0);
		}
		GraphIndexing graphIndexing = new GraphIndexing(activeUsers);
		File outDir = new File(graphIndexing.outDir);
		System.out.println(outDir.getAbsolutePath() + "\t" + outDir.exists()
				+ "\n");
		if (!outDir.exists()) {
			outDir.mkdir();
		}
		TreeMap<Integer, UserFollowers> map = graphIndexing.buildMap();
		System.out.println("buildMap finished .. start writing map with size: "
				+ map.size());
		writeHashMap("graphMap", map);
	}
}