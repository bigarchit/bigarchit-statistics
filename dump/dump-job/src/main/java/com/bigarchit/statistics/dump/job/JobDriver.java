package com.bigarchit.statistics.dump.job;

import org.apache.hadoop.util.ProgramDriver;

public class JobDriver {
	public static void main(String[] args){
		int exitCode = -1;
		ProgramDriver driver = new  ProgramDriver();
		try{
			driver.addClass("dump", DumpJob.class, "begin the dump job");
			driver.addClass("sync", SyncJob.class, "clear the dumped files or create the dumped files");
			
			driver.driver(args);
			exitCode = 0;
		}catch (Throwable e) {
			e.printStackTrace();
		}
		System.exit(exitCode);
	}
}
