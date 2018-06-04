package fr.telecom.paristech.slave;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Stack;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import fr.telecom.paristech.Constants;

public class Slave {
	public static void Map() {
		//System.out.println("Map");
		// Know other slaves : Read slaves.txt
		ArrayList<String> slavesList = new ArrayList<String>();
		BufferedReader br;
		try {
			br = new BufferedReader(new FileReader(Constants.BASE_DIRECTORY + "slaves.txt"));
			String line = br.readLine();
			while (line != null) {
				slavesList.add(line);
				line = br.readLine();
			}
			br.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		int slavesCount = slavesList.size();
		ConcurrentHashMap<Integer, BlockingQueue<String>> wordSplitHashMap = new ConcurrentHashMap<Integer, BlockingQueue<String>>();
		// Initialize concurrent Hashmap for each slave
		for (int i = 0; i < slavesCount; i++) {
			wordSplitHashMap.put(i, new ArrayBlockingQueue<String>(1000000));
		}

		// Find all files that were assigned for map stage
		// File folder = new File("/home/bambi/source/INF727/splits");
		File folder = new File(Constants.SPLITS_DIRECTORY);
		File[] listOfFiles = folder.listFiles();

		Stack<Thread> threadStack = new Stack<Thread>();
		// wordCount: call wcThread on each file
		for (File file : listOfFiles) {
			if (file.isFile()) {
				WordCountThread wcThread = new WordCountThread(file, slavesCount, wordSplitHashMap);
				threadStack.push(wcThread);
				wcThread.start();
			}
		}
		while (!threadStack.isEmpty()) {
			try {
				threadStack.pop().join();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		// now save the file: hostname_split_filename.txt

		ArrayList<String> stdoutArray = new ArrayList<String>();
		for (Integer key : wordSplitHashMap.keySet()) {
			// do not create a file if there is nothing to push
			if (!wordSplitHashMap.get(key).isEmpty()) {
				try {
					// PrintWriter writer = new PrintWriter("/home/bambi/source/INF727/maps/" + "UM"
					// + String.valueOf(key) + ".txt", "UTF-8");
					PrintWriter writer = new PrintWriter(Constants.MAPS_DIRECTORY + "UM" + String.valueOf(key) + ".txt",
							"UTF-8");
					stdoutArray.add("UM" + String.valueOf(key) + ".txt");
					while (!wordSplitHashMap.get(key).isEmpty()) {
						writer.println(wordSplitHashMap.get(key).take());
					}
					writer.close();
					// PrintWriter writer2 = new PrintWriter("/home/bambi/source/INF727/maps/" +
					// "OK" + ".txt", "UTF-8");
					PrintWriter writer2 = new PrintWriter(Constants.MAPS_DIRECTORY + "OK" + ".txt", "UTF-8");
					writer2.close();
				} catch (FileNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					System.err.println("File not found: " + "UM" + String.valueOf(key) + ".txt");
				} catch (UnsupportedEncodingException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					System.err.println("Encoding trouble: " + "UM" + String.valueOf(key) + ".txt");
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					System.err.println("Interruption: " + "UM" + String.valueOf(key) + ".txt");
				}

			}
		}
		System.out.println(String.join(";", stdoutArray));
	}

	public static void Reduce() {
		//System.out.println("Reduce");
		// Find all files that were assigned for reduce stage
		// File folder = new File("/home/bambi/source/INF727/splits");
		File folder = new File(Constants.SHUFFLE_DIRECTORY);
		File[] listOfFiles = folder.listFiles();

		ConcurrentHashMap<String, AtomicLong> wordSplitHashMap = new ConcurrentHashMap<String, AtomicLong>();
		Stack<Thread> threadStack = new Stack<Thread>();
		// wordCount: call rThread on each file
		for (File file : listOfFiles) {
			if (file.isFile()) {
				ReduceThread rThread = new ReduceThread(file, wordSplitHashMap);
				threadStack.push(rThread);
				rThread.start();
			}
		}
		while (!threadStack.isEmpty()) {
			try {
				threadStack.pop().join();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		// now send the output
		// First version : create a file even if there is no word (technical validation purpose)
//		PrintWriter writer;
//		try {
//			writer = new PrintWriter(Constants.REDUCE_DIRECTORY + "SM" + ".txt", "UTF-8");
//			for (String key: wordSplitHashMap.keySet()) {
//				writer.println(key + " " + wordSplitHashMap.get(key));
//			}
//			writer.close();
//		} catch (FileNotFoundException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//			System.err.println("Interruption: " + "SM" + ".txt");
//		} catch (UnsupportedEncodingException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//			System.err.println("Encoding trouble: " + "SM" + ".txt");
//		}

//		System.out.println(Constants.REDUCE_DIRECTORY + "SM" + ".txt");
		
		//2nd version: do not create a file but send wordcounts via stdout
		for (String key: wordSplitHashMap.keySet()) {
			System.out.println(key + " " + wordSplitHashMap.get(key));
		}
		
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
//		System.out.println("Program Arguments:");
//        for (String arg : args) {
//            System.out.println("\t" + arg);
//        }
        
        String parameter = System.getProperty("step"); 
		if ("0".equals(parameter)) {
			Map();
		} else if ("1".equals(parameter)) {
			Reduce();
		} else {
		System.err.println("wrong parameter: " + parameter);
		}

	}

}