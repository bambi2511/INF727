package fr.telecom.paristech.deploy;

import fr.telecom.paristech.master.SlaveSwarm;

public class Deploy {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		// Step 1: get usable hosts from file
		String hostFile = "/home/bambi/source/INF727/c130_test.txt";
		//String hostFile = "/home/bambi/source/INF727/c1XX_machines.txt";
		SlaveSwarm sw = new SlaveSwarm(hostFile);
		System.out.println(sw.getStatus());
		
		sw.setHosts();
		System.out.println(sw.gethost());
		System.out.println(sw.getStatus());

		// Check if they are reachable via ssh 
		System.out.println("TEST CONNECTIVITY");
		sw.setAvailableHosts(700);
		System.out.println(sw.getAvailableHostList());
		System.out.println(sw.getStatus());

		// Create temp directories
		System.out.println("CREATE FOLDERS");
		sw.setTempDirectory(4000);
		System.out.println(sw.getStatus());
		
		// Step 2 : copy slave.jar
		System.out.println("COPY SLAVE.JAR");
		String binaryPath = "/home/bambi/workspace/MapReduce/target/";
		sw.setBinary(binaryPath, 4000);
		System.out.println(sw.getStatus());
		
		// Step 3: copy data files
		System.out.println("COPY DATA FILES");
		String dataPath = "/home/bambi/source/INF727/splits/";
		sw.sendFiles(dataPath, 5000);
		System.out.println(sw.getFileHostHashMap());
		System.out.println(sw.getStatus());
		
		// Step 4: Map step: 
		System.out.println("MAP STEP");
		sw.startMap(30000);
		System.out.println(sw.getHostUMHashMap());
		
		// Step 5: Shuffle step
		System.out.println("SHUFFLE STEP");
		sw.startShuffle(10000);
		
		// Step 6: Reduce Step
		String reducePath = "/home/bambi/source/INF727/results/";
		System.out.println("REDUCE STEP");
		sw.startReduce(reducePath, 30000);
	}

}
