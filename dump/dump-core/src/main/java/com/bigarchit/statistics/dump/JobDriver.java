package com.bigarchit.statistics.dump;

import org.apache.hadoop.util.ProgramDriver;

public class JobDriver {
	public static void main(String[] args){
		int exitCode = -1;
		ProgramDriver driver = new  ProgramDriver();
		try{
			driver.addClass("DumpJob", DumpJob.class, "dump job");
			driver.addClass("SyncJob", SyncJob.class, "sync job");
			
			driver.driver(args);
			exitCode = 0;
		}catch (Throwable e) {
			e.printStackTrace();
		}
		System.exit(exitCode);
	}
}
