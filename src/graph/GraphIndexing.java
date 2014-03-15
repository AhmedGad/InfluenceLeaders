package graph;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashSet;
import java.util.StringTokenizer;

public class GraphIndexing {

	static byte[] bytes = new byte[600000000];

	public final static int READ_FOLLOWERS = 0;
	public final static int NEW_USER = 1;
	public final static int FIRST_LINE_AFTER_NEW_USER = 2;
	public final static String indexedDir = "/media/hanafy/New Volume/Lectures/4th year/GP/Data Collected/Indexed/";
	public final static String setsDir = "/media/hanafy/New Volume/Lectures/4th year/GP/Data Collected/";
	public final static String graphDir = "/media/hanafy/New Volume/Lectures/4th year/GP/Data Collected/Graph";

	@SuppressWarnings("resource")
	public static void main(String[] args) throws Exception {
		File inputFile = new File(graphDir);

		HashSet<String> fileSet = readHashSet(setsDir);
		for (File f : inputFile.listFiles()) {
			if (f.getName().startsWith("graph") && f.getName().endsWith(".txt") && !fileSet.contains(f.getName())) {
				System.out.println(f.getAbsolutePath());

				int state = NEW_USER;
				int NumUsers = 0;

				long t1 = System.currentTimeMillis();
				FileInputStream fis;
				DataInputStream dis = new DataInputStream(fis = new FileInputStream(f));
				int size = dis.available(), loaded = dis.read(bytes);
				if (size != loaded) {
					throw new Exception(f.getAbsolutePath() + " loaded: " + loaded + " total Size: " + size);
				}

				ByteArrayInputStream in = new ByteArrayInputStream(bytes, 0, size);
				BufferedReader buff = new BufferedReader(new InputStreamReader(in));

				BufferedWriter writer = null;

				while (true) {
					String s = buff.readLine();
					if (s == null) {
						if (writer != null)
							writer.close();
						break;
					}
					if (state == READ_FOLLOWERS) {
						if (s.length() == 0) {
							writer.close();
							state++;
						} else {
							writer.write(s + "\n");
						}
					} else if (state == NEW_USER) {
						NumUsers++;
						StringTokenizer tok = new StringTokenizer(s);
						writer = new BufferedWriter(new FileWriter(new File(indexedDir + tok.nextToken())));
						if (!tok.nextToken().equals("followers:")) {
							throw new Exception("\"followers:\" word not found!!" + "\tcur line: " + s);
						}
						state++;
					} else if (state == FIRST_LINE_AFTER_NEW_USER) {
						if (s.length() > 0)
							throw new Exception("first line is not empty!!!!" + "\tcur line: " + s);
						state = 0;
					}
				}

				fis.close();
				dis.close();
				System.out.println(NumUsers + " " + (System.currentTimeMillis() - t1));

				// write HashSet
				fileSet.add(f.getName());
				writeHashSet(fileSet, setsDir);
			}
		}

		System.out.println("Finished");
	}

	private static void writeHashSet(HashSet<String> set, String directory) throws IOException {
		String dir = directory + "graphSet";
		FileOutputStream fout = new FileOutputStream(dir);
		ObjectOutputStream oos = new ObjectOutputStream(fout);
		oos.writeObject(set);

		oos.close();
		fout.close();
	}

	private static HashSet<String> readHashSet(String directory) throws IOException, ClassNotFoundException {
		String dir = directory + "graphSet";
		FileInputStream fin;
		try {
			fin = new FileInputStream(dir);
		} catch (FileNotFoundException e) {
			writeHashSet(new HashSet<String>(), directory);
			fin = new FileInputStream(dir);
		}

		ObjectInputStream ois = new ObjectInputStream(fin);
		HashSet<String> set = (HashSet<String>) ois.readObject();
		ois.close();

		return set;
	}
}