package dataPreparation;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.HashSet;

public class GraphTrimmer {
	File[] fileList;
	byte[] buffArr;
	String outDir;
	HashSet<Long> set;

	/**
	 * VI Note: RUN ActiveUserExtractor FIRST.
	 * 
	 * Delete inactive followers from the graph. If outDir = graphDir then
	 * trimmed graph will override the original graph.
	 * 
	 */
	public GraphTrimmer(String graphDir, String outDir, File activeUsers,
			int maxUserFileSize) throws IOException {
		fileList = new File(graphDir).listFiles();
		buffArr = new byte[maxUserFileSize];
		this.outDir = outDir;
		set = new HashSet<Long>();
		BufferedReader reader = new BufferedReader(new FileReader(activeUsers));
		String s;
		while ((s = reader.readLine()) != null) {
			set.add(Long.parseLong(s));
		}
		reader.close();
	}

	public void start() throws IOException {
		System.out.println("GraphTrimmer started!");
		PrintWriter errorLogWriter = new PrintWriter(new File(
				"./logs/GraphTrimmer-error-log.txt"));
		PrintWriter logWriter = new PrintWriter("./logs/GraphTrimmer-log.txt");
		int beforeCnt = 0, afterCnt = 0, finishedUsers = 0;

		for (File file : fileList) {
			try {
				Long.parseLong(file.getName());
			} catch (Exception e) {
				continue;
			}
			logWriter.println(file.getName() + " -> started");
			logWriter.flush();

			FileInputStream fis = new FileInputStream(file);
			DataInputStream dis = new DataInputStream(fis);
			int size = dis.read(buffArr);
			if (dis.available() > 0) {
				System.err.println(file.getName() + " not red completely!!");
				errorLogWriter
						.println(file.getName() + " not red completely!!");
				errorLogWriter.flush();
			}
			fis.close();
			dis.close();
			BufferedReader reader = new BufferedReader(new InputStreamReader(
					new ByteArrayInputStream(buffArr, 0, size)));
			BufferedWriter writer = new BufferedWriter(new FileWriter(new File(
					outDir + file.getName())));
			String s;
			while ((s = reader.readLine()) != null) {
				beforeCnt++;
				if (set.contains(Long.parseLong(s))) {
					writer.write(s + "\n");
					afterCnt++;
				}
			}
			reader.close();
			writer.close();
			logWriter.write(file.getName() + "\tfinished.\n");
			logWriter.flush();
			finishedUsers++;
			if (finishedUsers % 10000 == 0) {
				System.out.println(finishedUsers
						+ " files finished, beforeCnt: " + beforeCnt
						+ ", afterCnt: " + afterCnt);
			}
		}
		// Percentage
		logWriter.write("\n\n*****\tFinished! .. beforeCnt: " + beforeCnt
				+ ", afterCnt: " + afterCnt + ", trimmed Percentage: "
				+ afterCnt * 100 / beforeCnt + "%\n");
		System.out.println("\n\n*****\tFinished! .. beforeCnt: " + beforeCnt
				+ ", afterCnt: " + afterCnt + ", trimmed-to-original Percentage: "
				+ afterCnt * 100 / beforeCnt + "%");
		errorLogWriter.close();
		logWriter.close();
	}

	public static void main(String[] args) throws IOException {
		File activeUsersFile = new File("activeUser.txt");
		if (!activeUsersFile.exists()) {
			throw new IOException("Active users file does not Exists!!");
		}
		String outDIr = "./Users-trimmed/";
		if (!new File(outDIr).exists()) {
			new File(outDIr).mkdir();
		}
		GraphTrimmer graphTrimmer = new GraphTrimmer("./Users/", outDIr,
				new File("activeUser.txt"), 800000000);
		graphTrimmer.start();
	}
}
