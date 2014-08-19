package com.bigarchit.statistics.dump.mapper;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import com.bigarchit.statistics.dump.DUMPContext;
import com.bigarchit.statistics.dump.Dumper;
import com.bigarchit.statistics.dump.KVPair;

public class DefaultMapper extends Mapper<Text, Text, Object, Object> {
	
	private static final Logger logger = Logger.getLogger(DefaultMapper.class);
	private static List<Dumper<Object, Object>> dumpers = DUMPContext.get().getDumpers();
	
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
					context.getCounter("staitcs-dump", "writeLine").increment(1);
				}
			} catch (Exception e) {
				logger.error("error", e);
			}
		}
	}
}
