package fr.telecom.paristech;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import fr.telecom.paristech.ProgramOutput.Status;

public class ShellCommandThread extends Thread{
	private ConcurrentHashMap<String,ProgramOutput> concurrentHashMap = null;
	private List<String> command = null;
	private int timeout = 1 * 1000;  //ms to s
	private String commandId = null;

	public ShellCommandThread(ConcurrentHashMap<String,ProgramOutput> concurrentHashMap, String commandId, List<String> command) {
		this.concurrentHashMap = concurrentHashMap;
		this.command = command;
		this.commandId = commandId;
	}
	
	public ShellCommandThread(ConcurrentHashMap<String,ProgramOutput> concurrentHashMap, String commandId, List<String> command, int timeout) {
		this.concurrentHashMap = concurrentHashMap;
		this.command = command;
		this.timeout = timeout; //ms
		this.commandId = commandId;
	}
	
	public void run() {
		ProcessBuilder builder = new ProcessBuilder(command);
		ArrayList<String> stdOutList = new ArrayList<String>();
		ArrayList<String> stdErrList = new ArrayList<String>();
		
		try {
			Process process = builder.start();
			ListenThread outThread = new ListenThread(process.getInputStream(), stdOutList);
			ListenThread errThread = new ListenThread(process.getErrorStream(), stdErrList);
			outThread.start();
			errThread.start();
			
			// wait for the program completion
			outThread.join(timeout);
			errThread.join(timeout);
			
			if (stdOutList.isEmpty() && stdErrList.isEmpty()) {
				concurrentHashMap.putIfAbsent(commandId, new ProgramOutput(Status.TIMEOUT, "", ""));
				//System.out.println(commandId + Status.TIMEOUT);
			} else if (!stdErrList.isEmpty()){
				concurrentHashMap.putIfAbsent(commandId, new ProgramOutput(Status.RUN_WITH_ERROR, String.join("\n", stdOutList), String.join("\n", stdErrList)));
				//System.out.println(commandId + Status.RUN_WITH_ERROR +  String.join("\n", stdErrList));
			} else {
				concurrentHashMap.putIfAbsent(commandId, new ProgramOutput(Status.RUN_OK, String.join("\n", stdOutList), String.join("\n", stdErrList)));
				//System.out.println(commandId + Status.RUN_OK +  String.join("\n", stdOutList));
			}
			
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		
	}
}
