package com.bigarchit.statistics.dump;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Reducer;

public interface Dumper<Key, Value> {

	void setName(String name);

	String getName();

	void setConfig(Configuration conf);

	Configuration getConfig();

	void setMapperClass(Class<Mapper<Writable, Writable, Writable, Writable>> mapperClass);
	
	Class<Mapper<Writable, Writable, Writable, Writable>> getMapperClass();
	
	void setReducerClass(Class<Reducer<Writable, Writable, Writable, Writable>> reducerClass);
	
	Class<Reducer<Writable, Writable, Writable, Writable>> getReducerClass();
	
	void setInput(String input);

	String getInput();

	void setOutput(String output);

	String getOutput();
	
	void setInputFormat(Class<InputFormat<Writable, Writable>> inputformat);
	
	Class<InputFormat<Writable, Writable>> getInputFormat();
	
	void setOutputFormat(Class<OutputFormat<Writable, Writable>> outputformat);
	
	Class<OutputFormat<Writable, Writable>> getOutputFormat();
	
	void beforeProcess();
	void beforeProcessPath(PathFiles pf);

	
	KVPair<Key, Value> write(Text key, Text value, Mapper<Text, Text, Key, Value>.Context context);
	
	
	void afterProcessPath(PathFiles pf);
	void afterProcess();
}
