package com.bigarchit.statistics.dump.mapping;

import java.net.UnknownHostException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.log4j.Logger;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.bson.types.ObjectId;

import com.bigarchit.statistics.dump.DUMPContext;
import com.bigarchit.statistics.dump.Dumper;
import com.bigarchit.statistics.dump.KVPair;
import com.bigarchit.statistics.dump.PathFiles;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.Mongo;
import com.mongodb.MongoException;
import com.mongodb.MongoURI;

public class DefaultMapping implements Dumper<Text, BSONObject> {
	private static final Logger logger = Logger.getLogger(DefaultMapping.class);

	private String name;

	private String input;

	private String output;

	private Configuration conf;

	private Class<InputFormat<Writable, Writable>> inputformat;
	
	private Class<OutputFormat<Writable, Writable>> outputformat;

	private Mongo mongo;

	public void setMongo(Mongo mongo) {
		this.mongo = mongo;
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

	@SuppressWarnings("deprecation")
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
				
				String uri = conf.get(DUMPContext.MONGODBMAPPING_MONGO_URI_KEY) + "/" + database + "." + collection;
				MongoURI mongoURI = new MongoURI(uri);
	
				logger.info("creating " + mongoURI);
				try {
					Mongo mongoIns = null;
					if (mongo == null) {
						mongoIns = new Mongo(mongoURI);
					} else {
						mongoIns = mongo;
					}
	
					DB db = mongoIns.getDB(mongoURI.getDatabase());
					DBCollection coll = db.getCollection(mongoURI.getCollection());
	
					coll.remove(new BasicDBObject(DUMPContext.MONGODBMAPPING_DATETIME_KEY, datetime));
					logger.info(coll + "(" + datetime + ")  had removed");
	
					conf.set(DUMPContext.MONGO_OUTPUT_URI_KEY, uri);
					conf.set(DUMPContext.MONGODBMAPPING_DATETIME_CONF, datetime);
				} catch (MongoException e) {
					logger.error(e);
					throw new RuntimeException(e);
				} catch (UnknownHostException e) {
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
	public KVPair<Text, BSONObject> write(Text key, Text value,
			Mapper<Text, Text, Text, BSONObject>.Context context) {
		if (key.toString().indexOf(DUMPContext.FOR_FIELDSPLIT) > -1) {
			BSONObject json = new BasicBSONObject();

			String[] fields = key.toString().split(DUMPContext.FOR_FIELDSPLIT_BY_SPLIT);
			for (int i = 0; i < fields.length; i++) {
				String field = fields[i];
				if (field.indexOf(DUMPContext.FOR_VALUESPLIT) < 0) {
					field = String.format("F%s::", i + 1) + field;
				}
				String[] kv = field.split(DUMPContext.FOR_VALUESPLIT);
				String k = kv[0];
				Object v = null;
				if(kv.length > 1){
					v = isInteger(kv[1]) ? Integer.parseInt(kv[1]) : (isDouble(kv[1]) ? Double.parseDouble(kv[1]) : kv[1]);
				}
				json.put(k, v);
			}
			if (fields.length > 0) {
				json.put(DUMPContext.MONGODBMAPPING_DATETIME_KEY, Integer.parseInt(context.getConfiguration().get(DUMPContext.MONGODBMAPPING_DATETIME_CONF)));
				return new KVPair<Text, BSONObject>(new Text(new ObjectId().toStringMongod()), json);
			}
		}
		return null;
	}
	
	
	@Override
	public void afterProcessPath(PathFiles pf) {
		
	}
	@Override
	public void beforeProcess() {
		
	}

	@Override
	public void afterProcess() {
		
	}

	@Override
	public String toString() {
		return "MongoDBMapping [name=" + name + ", input=" + input
				+ ", output=" + output + ", conf=" + conf + ", outputformat="
				+ outputformat + ", mongo=" + mongo + "]";
	}

		

	
}
