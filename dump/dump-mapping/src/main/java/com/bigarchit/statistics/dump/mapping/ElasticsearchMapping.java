package com.bigarchit.statistics.dump.mapping;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;

import com.bigarchit.statistics.dump.KVPair;
import com.bigarchit.statistics.dump.PathFiles;
import com.bigarchit.statistics.dump.inf.Dumper;

public class ElasticsearchMapping implements Dumper<Text, Object>{

	private String name;
	private String input;
	private String output;
	private Configuration conf;
	private Class<InputFormat<Writable, Writable>> inputformat;
	private Class<OutputFormat<Writable, Writable>> outputformat;
	
	@Override
	public void setName(String name) {
		this.name = name;
	}

	@Override
	public String getName() {
		return name;
	}

	@Override
	public void setConfig(Configuration conf) {
		this.conf = conf;
	}

	@Override
	public Configuration getConfig() {
		return conf;
	}

	@Override
	public void setInput(String input) {
		this.input = input;
	}

	@Override
	public String getInput() {
		return input;
	}

	@Override
	public void setOutput(String output) {
		this.output = output;
	}

	@Override
	public String getOutput() {
		return output;
	}

	@Override
	public void setInputFormat(Class<InputFormat<Writable, Writable>> inputformat) {
		this.inputformat = inputformat;
	}

	@Override
	public Class<InputFormat<Writable, Writable>> getInputFormat() {
		return inputformat;
	}

	@Override
	public void setOutputFormat(Class<OutputFormat<Writable, Writable>> outputformat) {
		this.outputformat = outputformat;
	}

	@Override
	public Class<OutputFormat<Writable, Writable>> getOutputFormat() {
		return outputformat;
	}

	@Override
	public void beforeProcess() {
		
	}

	@Override
	public void beforeProcessPath(PathFiles pf) {
		
	}

	@Override
	public KVPair<Text, Object> write(Text key, Text value, Mapper<Text, Text, Text, Object>.Context context) {
		return null;
	}

	@Override
	public void afterProcessPath(PathFiles pf) {
		
	}

	@Override
	public void afterProcess() {
		
	}
	
	
}
