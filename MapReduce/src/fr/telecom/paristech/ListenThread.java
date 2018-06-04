package fr.telecom.paristech;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;

public class ListenThread extends Thread{
	private ArrayList<String> outputArrayList = null;
	private InputStream inputStream = null;
	
	public ListenThread(InputStream inputStream, ArrayList<String> outputArrayList) {
		this.outputArrayList = outputArrayList;
		this.inputStream = inputStream;
	}
	
	public void run() {
		//System.out.println("Begin thread" + inputStream.toString());
		
		BufferedReader inputStreamReader = new BufferedReader(new InputStreamReader(inputStream));
		
		//recupere la sortie standard ou erreur
		try {
			String bufferedLine;
			while ((bufferedLine = inputStreamReader.readLine()) != null) {
				outputArrayList.add(bufferedLine);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		//System.out.println("End thread" + inputStream.toString());
	}
}
