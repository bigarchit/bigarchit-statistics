package com.bigarchit.statistics.dump;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import com.bigarchit.statistics.dump.util.PathFilesUtil;

public class DumpJob {
	private static final Logger logger = Logger.getLogger(DumpJob.class);

	private static FileSystem hdfs;

	private static List<Dumper<Object, Object>> dumpers = ANTContext.get().getDumpers();

	public static void setDumpers(List<Dumper<Object, Object>> dumpers) {
		DumpJob.dumpers = dumpers;
	}

	public static void setHdfs(FileSystem hdfs) {
		DumpJob.hdfs = hdfs;
	}

	public static class ANTDumpMapper extends Mapper<Text, Text, Object, Object> {
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
		}

		@Override
		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			for (Dumper<Object, Object> dumper : dumpers) {
				try {
					KVPair<Object, Object> kv = dumper.write(key, value, context);
					if (kv != null) {
						context.write(kv.key, kv.value);
						context.getCounter("", "writeLine").increment(1);
					}
				} catch (Exception e) {
					logger.error("error", e);
				}
			}
		}
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
					Job job = dumper.getJob(pf);
					
					if(job != null){
						job.waitForCompletion(true);
						FSDataOutputStream out = hdfs.create(new Path(pf.path + "/_" + dumper.getName().toUpperCase() + "_DUMP"), true);
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