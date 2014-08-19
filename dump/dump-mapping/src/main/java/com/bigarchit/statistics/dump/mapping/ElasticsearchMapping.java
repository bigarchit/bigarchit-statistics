package com.bigarchit.statistics.dump.mapping;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;
import org.bson.types.ObjectId;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

import com.bigarchit.statistics.dump.DUMPContext;
import com.bigarchit.statistics.dump.Dumper;
import com.bigarchit.statistics.dump.KVPair;
import com.bigarchit.statistics.dump.PathFiles;

public class ElasticsearchMapping implements Dumper<Text, MapWritable>{

	private static final Logger logger = Logger.getLogger(ElasticsearchMapping.class);
	
	private String name;
	private String input;
	private String output;
	private Configuration conf;
	private Class<InputFormat<Writable, Writable>> inputformat;
	private Class<OutputFormat<Writable, Writable>> outputformat;
	
	private Client esClient;
	
	
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
		Node node = NodeBuilder.nodeBuilder().clusterName(conf.get(DUMPContext.ES_CLUSTER_NAME)).node();
		esClient = node.client();
	}

	public static boolean isInteger(String str) {
		Pattern pattern = Pattern.compile("^[-\\+]?[\\d]*$");
		return pattern.matcher(str).matches();
	}

	public static boolean isDouble(String str) {
		Pattern pattern = Pattern.compile("^[-\\+]?[.\\d]*$");
		return pattern.matcher(str).matches();
	}
	
	
	@Override
	public void beforeProcessPath(PathFiles pf) {
		try{
			String fixpath = pf.path.substring(input.length());
			fixpath = (fixpath.startsWith("/") ? "" : "/") + fixpath;
			fixpath = fixpath + (fixpath.endsWith("/") ? "" : "/");
			fixpath = fixpath.length() > 2 ? (fixpath.substring(1, fixpath.length() - 1)) : fixpath;
			logger.info("fixpath:[" + fixpath + "]");
			Pattern p = Pattern.compile("([^/]*)/([^/]*)/([^/]*)/([^/]*)/([^/]*)");
			Matcher m = p.matcher(fixpath);
			if (m.find()) {
				String database = m.group(1);
				String type = m.group(2);
				String datetime = m.group(3);
				String dem1 = m.group(4);
				String dem2 = m.group(5);
				
				
				if(database.isEmpty() || type.isEmpty() || datetime.isEmpty() || dem1.isEmpty() || dem2.isEmpty()){
					throw new RuntimeException("input not match mongoDB mapping");
				}
				
				String collection = database + "_" + type + "_" + datetime.length() + "_" + dem1 + "_" + dem2;
				
	
				try {
					esClient.prepareDeleteByQuery(database).setTypes(collection).setQuery(QueryBuilders.termQuery(DUMPContext.DATETIME_KEY, datetime)).execute().actionGet();
					logger.info(String.format("%s (%s) had removed", collection, datetime));
					
					conf.set(DUMPContext.ES_CLUSTER_NAME, String.format("%s/%s", datetime, collection));
					conf.set(DUMPContext.DATETIME_KEY, datetime);
				} catch (Exception e) {
					logger.error(e);
					throw new RuntimeException(e);
				} 
			} else {
				throw new RuntimeException("input not match mongoDB mapping");
			}
		}catch(Exception e){
			throw new RuntimeException("input not match mongoDB mapping");
		}
		
	}

	@Override
	public KVPair<Text, MapWritable> write(Text key, Text value, Mapper<Text, Text, Text, MapWritable>.Context context) {
		if (key.toString().indexOf(DUMPContext.FOR_FIELDSPLIT) > -1) {
			MapWritable map = new MapWritable();
			String[] fields = key.toString().split(DUMPContext.FOR_FIELDSPLIT_BY_SPLIT);
			for (int i = 0; i < fields.length; i++) {
				String field = fields[i];
				if (field.indexOf(DUMPContext.FOR_VALUESPLIT) < 0) {
					field = String.format("F%s::", i + 1) + field;
				}
				String[] kv = field.split(DUMPContext.FOR_VALUESPLIT);
				String k = kv[0];
				Writable v = null;
				if(kv.length > 1){
					v = isInteger(kv[1]) ? new IntWritable(Integer.parseInt(kv[1])) : (isDouble(kv[1]) ? new DoubleWritable(Double.parseDouble(kv[1])) : new Text(kv[1]));
				}
				map.put(new Text(k), v);
			}
			if (fields.length > 0) {
				map.put(new Text(DUMPContext.DATETIME_KEY), new IntWritable(Integer.parseInt(context.getConfiguration().get(DUMPContext.MONGODBMAPPING_DATETIME_CONF))));
				return new KVPair<Text, MapWritable>(new Text(new ObjectId().toStringMongod()), map);
			}
		}
		return null;
	}

	@Override
	public void afterProcessPath(PathFiles pf) {
		
	}

	@Override
	public void afterProcess() {
		
	}

	@Override
	public void setMapperClass(Class<Mapper<Writable, Writable, Writable, Writable>> mapperClass) {
	}

	@Override
	public Class<Mapper<Writable, Writable, Writable, Writable>> getMapperClass() {
		return null;
	}

	@Override
	public void setReducerClass(Class<Reducer<Writable, Writable, Writable, Writable>> reducerClass) {
	}

	@Override
	public Class<Reducer<Writable, Writable, Writable, Writable>> getReducerClass() {
		return null;
	}
	
	
}
