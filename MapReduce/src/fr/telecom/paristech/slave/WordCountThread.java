package fr.telecom.paristech.slave;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

public class WordCountThread extends Thread{
	private File file = null;
	private int slavesCount = 0;
	private ConcurrentHashMap<Integer,BlockingQueue<String>> wordSplitHashMap = null;
	
	public WordCountThread(File file, int slavesCount, ConcurrentHashMap<Integer,BlockingQueue<String>> wordSplitHashMap) {
		this.file = file;
		this.slavesCount = slavesCount;
		this.wordSplitHashMap = wordSplitHashMap;
	}
	
	public void run() {
		try {
			BufferedReader reader = new BufferedReader(new FileReader(file));
			// assign the word to the right slave
			Integer slaveNbr = -1;
			String line = reader.readLine();
			while (line != null) {
				for(String word : line.split(" ")){
					//Calculate word hashcode (positive) % slavesCount to know which slave will gather words at the shuffle step
					slaveNbr = (word.hashCode() & 0xfffffff) % slavesCount;
					wordSplitHashMap.get(slaveNbr).put(word);
				}
				line = reader.readLine();
			}
			reader.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
