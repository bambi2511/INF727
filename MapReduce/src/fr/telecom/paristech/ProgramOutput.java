package fr.telecom.paristech;

public class ProgramOutput {
	private Status stdStatus= null;
	private String stdOut="";
	private String stdErr="";
	
	public enum Status {
        NOT_YET_RUN, RUN_OK, RUN_WITH_ERROR, TIMEOUT
    }
	
	public ProgramOutput(Status stdStatus, String stdOut, String stdErr) {
		this.stdStatus = stdStatus;
		this.stdOut = stdOut;
		this.stdErr = stdErr;
	}
	
	public Status getStdStatus() {
		return this.stdStatus;
	}
	
	public String getStdOut() {
		return this.stdOut;
	}
	
	public String getStdErr() {
		return this.stdErr;
	}
}
