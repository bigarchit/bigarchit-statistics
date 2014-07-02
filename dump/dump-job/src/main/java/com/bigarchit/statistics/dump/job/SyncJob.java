package com.bigarchit.statistics.dump.job;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.bigarchit.statistics.dump.util.PathFilesUtil;


public class SyncJob extends Configured implements Tool{
	
	private static final Logger logger = Logger.getLogger(SyncJob.class);
	
	private static FileSystem hdfs;
	
	@Override
	public int run(String[] args) throws Exception {
		String[] otherArgs = new GenericOptionsParser(getConf(), args) .getRemainingArgs();
		if (otherArgs.length != 3) {
			System.err.println("Usage: sync <path> <prefix> <option(clear, create)>");
			return 2;
		}
		
		String baseDir = args[0];
		baseDir += baseDir.endsWith("/") ? "" : "/";

		if (baseDir.indexOf("://") > 1) {
			int fpos = baseDir.indexOf("/", 8);
			String fsname = fpos > 1 ? baseDir.substring(0, fpos) : baseDir;
			getConf().set("fs.defaultFS", fsname);
			logger.info("found fsname:" + fsname);
		}
		hdfs = FileSystem.get(URI.create(baseDir), getConf());
		String prefix = args[1];
		String option = args[2];
		if(option.equalsIgnoreCase("create")) {
			PathFilesUtil.createTags(hdfs, prefix, new Path(baseDir));
		}else if(option.equalsIgnoreCase("clear")) {
			PathFilesUtil.clearTags(hdfs, prefix, new Path(baseDir));
		}
		return 0;
	}
	
	
	
	public static void main(String[] args) throws Exception{
		int res = ToolRunner.run(new Configuration(), new SyncJob(), args);
	    System.exit(res);
	}
	
}
