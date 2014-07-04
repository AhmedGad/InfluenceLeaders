package DataSummary;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;
import java.util.TreeMap;

public class DataMatcher {
	private static String testDataDirectory = "./Final/TotalData_Test.txt";
	private static String trainingDataDirectory = "./Final/TotalData_Training.txt";
	private static String outputDirectory = "./Final/TotalData_Matched.txt";
	private static String outputDirectory2 = "./Final/TotalData_NoMatched.txt";
	

	public static void main(String[] args) throws IOException {
		run();
	}

	private static void run() throws IOException {
		TreeMap<Long, User> testDataMap = loadtestDataInMemory();
		readTrainingDataAndMatch(testDataMap);
	}

	static HashSet<Long> oldSet = new HashSet<>();
	static HashSet<Long> newSet = new HashSet<>();
	static HashSet<Long> matchedSet = new HashSet<>();
	
	
	private static void readTrainingDataAndMatch(TreeMap<Long, User> testDataMap) throws IOException {
		System.out.println("Start Reading Training Data And Match");

		BufferedReader fileReader = new BufferedReader(new FileReader(trainingDataDirectory));
		BufferedWriter outputWriter = new BufferedWriter(new FileWriter(outputDirectory));
		// Train
		outputWriter.write("UserID,NumFollowers,NumFriends,NumTweets1,DateOfJoin,");
		outputWriter.write("AvgT1,MinT1,MaxT1,");
		outputWriter.write("AvgL1,MinL1,MaxL1,");
		
		// test
		outputWriter.write("NumTweets2,AvgT2,MinT2,MaxT2,");
		outputWriter.write("AvgL2,MinL2,MaxL2\n");
		
		
		int matched = 0, noMatch = 0;
		while (fileReader.ready()) {
			String line = fileReader.readLine();
			String oldData = parseData(line);
			long userID = parseID(line);
			oldSet.add(userID);
			
			if (testDataMap.containsKey(userID)) {
				matched++;
				String[] args = testDataMap.get(userID).data.split(",");
				
				outputWriter.write(userID + "," + oldData + ",");
				outputWriter.write(args[2] + ",");
				outputWriter.write(args[4] + ",");
				outputWriter.write(args[5] + ",");
				outputWriter.write(args[6] + ",");
				outputWriter.write(args[7] + ",");
				outputWriter.write(args[8] + ",");
				outputWriter.write(args[9] + "\n");
				matchedSet.add(userID);
			} else {
				noMatch++;
//				outputWriter.write(userID + ":" + oldData + " >> " + "NULL" + "\n");
			}
		}

		BufferedWriter notMatchedWriter = new BufferedWriter(new FileWriter(outputDirectory2));
		
		for(long id : testDataMap.keySet())
		{
			if(!matchedSet.contains(id))
				notMatchedWriter.write(id + "," + testDataMap.get(id).data + "\n");
		}
		
		notMatchedWriter.close();
		fileReader.close();
		outputWriter.close();

		System.out.println("MATCHED : " + matched + ", doesn't match : " + noMatch);
		System.out.println("OLD Users : " + oldSet.size());
		System.out.println("NEW Users : " + newSet.size());
		
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
			newSet.add(userID);
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
