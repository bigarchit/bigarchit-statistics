package com.bigarchit.statistics.dump.job;

import java.net.URI;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import com.bigarchit.statistics.dump.DUMPContext;
import com.bigarchit.statistics.dump.PathFiles;
import com.bigarchit.statistics.dump.inf.Dumper;
import com.bigarchit.statistics.dump.map.DefaultMapper;
import com.bigarchit.statistics.dump.util.PathFilesUtil;

public class DumpJob {
	private static final Logger logger = Logger.getLogger(DumpJob.class);

	private static FileSystem hdfs;

	private static List<Dumper<Object, Object>> dumpers = DUMPContext.get().getDumpers();

	public static void setDumpers(List<Dumper<Object, Object>> dumpers) {
		DumpJob.dumpers = dumpers;
	}

	public static void setHdfs(FileSystem hdfs) {
		DumpJob.hdfs = hdfs;
	}

	public void run() throws Exception {
		logger.info(String.format("Starting %s", new Object[] { getClass().getSimpleName() }));

		for (Dumper<Object, Object> dumper : dumpers) {
			logger.info("start " + dumper);
			
			dumper.beforeProcess();
			String baseDir = dumper.getInput();
			baseDir += baseDir.endsWith("/") ? "" : "/";

			if (baseDir.indexOf("://") > 1) {
				int fpos = baseDir.indexOf("/", 8);
				String fsname = fpos > 1 ? baseDir.substring(0, fpos) : baseDir;
				dumper.getConfig().set("fs.defaultFS", fsname);
				logger.info("found fsname:" + fsname);
			}else{
				
			}
			if(hdfs == null){
				hdfs = FileSystem.get(URI.create(baseDir), dumper.getConfig());
			}
			Set<PathFiles> pfs = PathFilesUtil.scanPath(hdfs, dumper.getName(), new Path(baseDir));
			for (PathFiles pf : pfs) {
				try{
					logger.info("scan " + pf);
					dumper.beforeProcessPath(pf);
					Job job = new Job(dumper.getConfig(), String.format("%s %s dumper", dumper.getName(), pf.path));
					job.setNumReduceTasks(0);
			
					job.setJarByClass(DumpJob.class);
					job.setMapperClass(DefaultMapper.class);
			
					job.setOutputKeyClass(Writable.class);
					job.setOutputValueClass(Writable.class);
			
					job.setInputFormatClass(dumper.getInputFormat());
					job.setOutputFormatClass(dumper.getOutputFormat());
			
					FileInputFormat.addInputPath(job, new Path(pf.path));
					FileOutputFormat.setOutputPath(job, new Path(dumper.getOutput()));
					
					if(job != null){
						job.waitForCompletion(true);
						FSDataOutputStream out = hdfs.create(new Path(pf.path + "/_" + dumper.getName().toUpperCase() + "_DUMPED"), true);
						out.close();
					}
					dumper.afterProcessPath(pf);
				}catch(Exception e){
					logger.error(String.format("job: %s dumper error", pf.path), e);
					continue;
				}
			}
			dumper.afterProcess();
		}
	}

	public static void main(String[] args) throws Exception {
		DumpJob job = new DumpJob();
		job.run();
	}
}