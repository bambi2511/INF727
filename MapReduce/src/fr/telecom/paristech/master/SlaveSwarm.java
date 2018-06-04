package fr.telecom.paristech.master;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Stack;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import fr.telecom.paristech.Constants;
import fr.telecom.paristech.ProgramOutput;
import fr.telecom.paristech.ShellCommandThread;

public class SlaveSwarm {

	public enum Status {
		NOT_YET_RUN, HOST_STANDBY, HOST_READY, HOST_DIR_SET_UP, HOST_EXE_DEPLOYED, HOST_FILE_DEPLOYED, MAP_OK, SHUFFLE_OK, REDUCE_OK, ERROR, TIMEOUT
	}

	private String hostFile;
	// hosts within input file
	private ArrayList<String> hostList = new ArrayList<String>();
	// hosts that are reachable
	private ArrayList<String> availableHostList = new ArrayList<String>();
	// hosts on which at least a text file is present
	private ArrayList<String> fileHostList = new ArrayList<String>();
	// dictionary: Sx -> host
	private HashMap<String, String> fileHostHashMap = new HashMap<String, String>();
	// dictionary: host -> UMx
	private HashMap<String, HashSet<String>> hostUMHashMap = new HashMap<String, HashSet<String>>();
	// Reducers list
	private HashSet<String> reducersSet = new HashSet<String>();
	// status
	private Status status = Status.NOT_YET_RUN;

	public SlaveSwarm(String hostFile) {
		this.hostFile = hostFile;
	}

	public void setHosts() {
		try {
			BufferedReader br = new BufferedReader(new FileReader(hostFile));
			String line = br.readLine();
			while (line != null) {
				hostList.add(line);
				line = br.readLine();
			}
			br.close();
			status = Status.HOST_STANDBY;
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			status = Status.ERROR;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			status = Status.ERROR;
		}
	}

	public void setAvailableHosts(Integer timeout) {
		ConcurrentHashMap<String, ProgramOutput> hostHashMap = new ConcurrentHashMap<String, ProgramOutput>();

		ArrayList<Thread> threadList = new ArrayList<Thread>();

		for (String host : hostList) {
			ShellCommandThread scThread = new ShellCommandThread(hostHashMap, host,
					Arrays.asList("ssh", Constants.USERNAME + "@" + host, "hostname"), timeout);
			scThread.run();
			threadList.add(scThread);
		}

		for (Thread thread : threadList) {
			try {
				thread.join();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		System.out.println(hostHashMap);

		for (ConcurrentHashMap.Entry<String, ProgramOutput> e : hostHashMap.entrySet()) {
			String key = e.getKey();
			ProgramOutput.Status value = e.getValue().getStdStatus();
			if (value == ProgramOutput.Status.RUN_OK) {
				availableHostList.add(key);
			}

		}
		// Write hosts to a file so that it can be send to the Slaves
		try {
			FileWriter writer = new FileWriter("/home/bambi/workspace/MapReduce/target/slaves.txt");
			for (String host : availableHostList) {
				writer.write(host);
				writer.write(System.getProperty("line.separator"));
			}
			writer.close();
			if (!availableHostList.isEmpty()) {
				status = Status.HOST_READY;
			} else {
				status = Status.ERROR;
			}
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
			status = Status.ERROR;
		}

	}

	public void setTempDirectory(Integer timeout) {
		ConcurrentHashMap<String, ProgramOutput> hostHashMap = new ConcurrentHashMap<String, ProgramOutput>();

		ArrayList<Thread> threadList = new ArrayList<Thread>();

		for (String host : availableHostList) {
			ShellCommandThread scThread = new ShellCommandThread(hostHashMap, host,
					Arrays.asList("/bin/sh", "-c", "ssh " + Constants.USERNAME + "@" + host +
					// " 'mkdir -p " + Constants.BASE_DIRECTORY + " &&" +
							" 'rm -rf " + Constants.BASE_DIRECTORY + " &&" + " mkdir -p " + Constants.BASE_DIRECTORY
							+ " &&" + " mkdir -p " + Constants.SPLITS_DIRECTORY + " &&" + " mkdir -p "
							+ Constants.MAPS_DIRECTORY + " &&" + " mkdir -p " + Constants.SHUFFLE_DIRECTORY + " &&"
							+ " mkdir -p " + Constants.REDUCE_DIRECTORY + " '"),
					timeout);

			scThread.run();
		}

		for (Thread thread : threadList) {
			try {
				thread.join();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		System.out.println(hostHashMap);

		status = Status.HOST_DIR_SET_UP;

	}

	public void setBinary(String binaryPath, Integer timeout) {
		ConcurrentHashMap<String, ProgramOutput> hostHashMap = new ConcurrentHashMap<String, ProgramOutput>();

		ArrayList<Thread> threadList = new ArrayList<Thread>();

		for (String host : availableHostList) {
			ShellCommandThread scThread1 = new ShellCommandThread(hostHashMap, host,
					Arrays.asList("/bin/sh", "-c", "scp -r -p " + binaryPath + "slave.jar " + Constants.USERNAME + "@"
							+ host + ":" + Constants.BASE_DIRECTORY),
					timeout);
			ShellCommandThread scThread2 = new ShellCommandThread(hostHashMap, host,
					Arrays.asList("/bin/sh", "-c", "scp -r -p " + binaryPath + "slaves.txt " + Constants.USERNAME + "@"
							+ host + ":" + Constants.BASE_DIRECTORY),
					timeout);
			scThread1.run();
			scThread2.run();
			threadList.add(scThread1);
			threadList.add(scThread2);
		}

		for (Thread thread : threadList) {
			try {
				thread.join();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		System.out.println(hostHashMap);

		status = Status.HOST_EXE_DEPLOYED;

	}

	public void sendFiles(String dataPath, Integer timeout) {

		File folder = new File(dataPath);
		File[] listOfFiles = folder.listFiles();

		// Create Hashmap: file -> hostname
		Stack<String> fileStack = new Stack<String>();
		for (File file : listOfFiles) {
			if (file.isFile()) {
				fileStack.push(file.getName());
			}
		}

		// Map each file to a hostname (cyclic assignment of a new hostname for each
		// file)

		int index = 0;
		String file = "";
		while (!fileStack.isEmpty()) {
			file = fileStack.pop();
			fileHostHashMap.put(file, availableHostList.get(index % availableHostList.size()));
			// List the slaves that have files
			if (index < availableHostList.size()) {
				fileHostList.add(availableHostList.get(index));
			}

			index++;
		}

		ConcurrentHashMap<String, ProgramOutput> hostHashMap = new ConcurrentHashMap<String, ProgramOutput>();
		ArrayList<Thread> threadList = new ArrayList<Thread>();

		for (Map.Entry<String, String> entry : fileHostHashMap.entrySet()) {
			ShellCommandThread scThread = new ShellCommandThread(hostHashMap, entry.getValue(),
					Arrays.asList("/bin/sh", "-c", "scp -r -p " + dataPath + entry.getKey() + " " + Constants.USERNAME
							+ "@" + entry.getValue() + ":" + Constants.SPLITS_DIRECTORY),
					timeout);
			scThread.run();
			threadList.add(scThread);
		}

		for (Thread thread : threadList) {
			try {
				thread.join();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		System.out.println(hostHashMap);

		status = Status.HOST_FILE_DEPLOYED;

	}

	public void startMap(Integer timeout) {
		ConcurrentHashMap<String, ProgramOutput> mapHashMap = new ConcurrentHashMap<String, ProgramOutput>();

		ArrayList<Thread> threadList = new ArrayList<Thread>();

		for (String host : fileHostList) {
			ShellCommandThread scThread = new ShellCommandThread(mapHashMap, host,
					Arrays.asList("/bin/sh", "-c", "ssh " + Constants.USERNAME + "@" + host + " 'java -Dstep=0 -jar "
							+ Constants.BASE_DIRECTORY + "slave.jar " + " '"),
					timeout);

			scThread.run();
		}

		for (Thread thread : threadList) {
			try {
				thread.join();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		System.out.println(mapHashMap);
		for (String host : mapHashMap.keySet()) {
			// System.out.println(host + mapHashMap.get(host).getStdOut());
			HashSet<String> UMHashSet = new HashSet<String>();
			for (String UM : mapHashMap.get(host).getStdOut().split(";")) {
				UMHashSet.add(UM);
			}
			hostUMHashMap.put(host, UMHashSet);
		}

		status = Status.MAP_OK;

	}

	public void startShuffle(Integer timeout) {
		ConcurrentHashMap<String, ProgramOutput> hostHashMap = new ConcurrentHashMap<String, ProgramOutput>();
		ArrayList<Thread> threadList = new ArrayList<Thread>();

		System.out.println(hostUMHashMap);
		for (String sourceHost : hostUMHashMap.keySet()) {
			for (String UM : hostUMHashMap.get(sourceHost)) {
				// ugly: to get slave number
				// System.out.println(UM);
				Pattern numPattern = Pattern.compile("[0-9]+");
				Matcher m = numPattern.matcher(UM);
				m.find();
				String destinationHost = availableHostList.get(Integer.valueOf(m.group(0)));
				// System.out.println(destinationHost);
				reducersSet.add(destinationHost);

				ShellCommandThread scThread = new ShellCommandThread(hostHashMap, sourceHost + UM,
						Arrays.asList("/bin/sh", "-c",
								"scp -r -p " + Constants.USERNAME + "@" + sourceHost + ":" + Constants.MAPS_DIRECTORY
										+ UM + " " + Constants.USERNAME + "@" + destinationHost + ":"
										+ Constants.SHUFFLE_DIRECTORY + sourceHost + "_" + UM),
						timeout);
				scThread.run();
				threadList.add(scThread);
			}
		}

		for (Thread thread : threadList) {
			try {
				thread.join();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		status = Status.SHUFFLE_OK;

	}

	public void startReduce(String reducePath,Integer timeout) {
		ConcurrentHashMap<String, ProgramOutput> reduceHashMap = new ConcurrentHashMap<String, ProgramOutput>();
		
		ArrayList<Thread> threadList = new ArrayList<Thread>();
		
		for (String host : reducersSet) {
            // second implementation with standard output
			ShellCommandThread scThread = new ShellCommandThread(reduceHashMap, host, Arrays.asList("/bin/sh", "-c", "ssh " + Constants.USERNAME + "@" + host + 
					" 'java -Dstep=1 -jar " + Constants.BASE_DIRECTORY + "slave.jar"  + " '"), 
					timeout);

			
			scThread.run();
		}
		
		
		for (Thread thread : threadList) {
			try {
				thread.join();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		// Now we concatenate all standard outputs
		FileWriter writer;
		try {
			writer = new FileWriter("/home/bambi/workspace/MapReduce/target/wordCount.txt");
			for (String host:reduceHashMap.keySet()) {
				System.out.println(host + " : " + reduceHashMap.get(host).getStdStatus());
				writer.write(reduceHashMap.get(host).getStdOut());
				//writer.write(System.getProperty( "line.separator" ));
			}
			writer.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		
		
		
		status = Status.REDUCE_OK;
		
	}

	public ArrayList<String> gethost() {
		return this.hostList;
	}

	public ArrayList<String> getAvailableHostList() {
		return this.availableHostList;
	}

	public HashMap<String, String> getFileHostHashMap() {
		return this.fileHostHashMap;
	}

	public Status getStatus() {
		return this.status;
	}

	public HashMap<String, HashSet<String>> getHostUMHashMap() {
		return this.hostUMHashMap;
	}

}
