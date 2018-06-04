package fr.telecom.paristech.slave;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class ReduceThread extends Thread{
	private File file = null;
	private ConcurrentHashMap<String,AtomicLong> wordSplitHashMap = null;
	
	public ReduceThread(File file, ConcurrentHashMap<String,AtomicLong> wordSplitHashMap) {
		this.file = file;
		this.wordSplitHashMap = wordSplitHashMap;
	}
	
	
	
	public void run() {
		try {
			BufferedReader reader = new BufferedReader(new FileReader(file));
			// assign the word to the right slave
			String word = "";
			while((word = reader.readLine()) != null) {
				wordSplitHashMap.putIfAbsent(word, new AtomicLong(0));
				wordSplitHashMap.get(word).incrementAndGet();
			}
			reader.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
