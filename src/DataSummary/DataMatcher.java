package DataSummary;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.TreeMap;

public class DataMatcher {
	private static String testDataDirectory = "./Final/TotalData_Test.txt";
	private static String trainingDataDirectory = "./Final/TotalData_Training.txt";
	private static String outputDirectory = "./Final/TotalData_Matched.txt";

	public static void main(String[] args) throws IOException {
		run();
	}

	private static void run() throws IOException {
		TreeMap<Long, User> testDataMap = loadtestDataInMemory();
		readTrainingDataAndMatch(testDataMap);
	}

	private static void readTrainingDataAndMatch(TreeMap<Long, User> testDataMap) throws IOException {
		System.out.println("Start Reading Training Data And Match");

		BufferedReader fileReader = new BufferedReader(new FileReader(trainingDataDirectory));
		BufferedWriter outputWriter = new BufferedWriter(new FileWriter(outputDirectory));

		while (fileReader.ready()) {
			String line = fileReader.readLine();
			String oldData = parseData(line);
			long userID = parseID(line);

			if (testDataMap.containsKey(userID)) {
				outputWriter.write(userID + "(old):" + oldData + "\n");
				outputWriter.write(userID + "(new):"  + testDataMap.get(userID).data + "\n\n");
			} else {
//				outputWriter.write(userID + ":" + oldData + " >> " + "NULL" + "\n");
			}
		}

		fileReader.close();
		outputWriter.close();

		System.out.println("Finish Matching");
	}

	private static TreeMap<Long, User> loadtestDataInMemory() throws IOException {
		System.out.println("Start Loading Test Data");

		TreeMap<Long, User> userMap = new TreeMap<Long, User>();
		BufferedReader fileReader = new BufferedReader(new FileReader(testDataDirectory));

		while (fileReader.ready()) {
			String line = fileReader.readLine();
			String data = parseData(line);
			long userID = parseID(line);
			userMap.put(userID, new User(data));
		}

		fileReader.close();

		System.out.println("Finish Loading Test Data");

		return userMap;
	}

	private static long parseID(String line) {
		int lastComma = line.lastIndexOf(',');
		long userID = Long.parseLong(line.substring(lastComma + 1));
		return userID;
	}

	private static String parseData(String line) {
		int lastComma = line.lastIndexOf(',');
		String data = line.substring(0, lastComma);
		return data;
	}

	private static class User {
		public String data;

		public User(String data) {
			this.data = data;
		}
	}
}
