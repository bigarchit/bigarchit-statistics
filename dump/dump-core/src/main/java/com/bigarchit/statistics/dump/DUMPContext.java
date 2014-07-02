package com.bigarchit.statistics.dump;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.log4j.Logger;

import com.bigarchit.statistics.dump.inf.Dumper;

public class DUMPContext {
	private static final Logger logger = Logger.getLogger(DUMPContext.class);

	public final static String LOG_SPILTKEY = ",";
	public final static String FOR_VALUESPLIT = "::";
	public final static String FOR_FIELDSPLIT = "||";
	public final static String FOR_FIELDSPLIT_BY_SPLIT = "\\|\\|";

	private static DUMPContext instance;

	public final static String DUMPER_KEY = "statistics.dumper";


	public final static String MONGODBMAPPING_DATETIME_KEY = "datetime";

	public final static String MONGODBMAPPING_MONGO_URI_KEY = "mapping.mongo.uri";

	public final static String MONGODBMAPPING_DATETIME_CONF = "mapping.datetime";

	public final static String MONGO_OUTPUT_URI_KEY = "mongo.output.uri";

	private static List<Dumper<Object, Object>> dumpers = new ArrayList<Dumper<Object, Object>>();

	private String formatTime(String path) {
		Date now = new Date();

		String yyyy = new SimpleDateFormat("yyyy").format(now);
		String MM = new SimpleDateFormat("MM").format(now);
		String dd = new SimpleDateFormat("dd").format(now);
		String HH = new SimpleDateFormat("HH").format(now);
		String mm = new SimpleDateFormat("mm").format(now);
		path = path.replace("${yyyy}", yyyy);
		path = path.replace("${MM}", MM);
		path = path.replace("${dd}", dd);
		path = path.replace("${HH}", HH);
		path = path.replace("${mm}", mm);
		return path;
	}

	@SuppressWarnings("unchecked")
	private DUMPContext(Configuration config) {
		try {
			if (config == null) {
				config = new PropertiesConfiguration("dump.properties");
			}

			Set<String> names = new LinkedHashSet<String>();
			Iterator<String> keys = config.getKeys(DUMPER_KEY);
			while (keys.hasNext()) {
				String key = keys.next();
				String name = key.substring(key.indexOf(".", DUMPER_KEY.length()) + 1, key.indexOf(".", DUMPER_KEY.length() + 1));
				names.add(name);
			}

			for (String name : names) {
				logger.info("found " + name);
				String prefix = DUMPER_KEY + "." + name;

				String cls = config.getString(prefix + ".class");
				Dumper<Object, Object> instance = (Dumper<Object, Object>) Class.forName(cls).newInstance();
				instance.setName(name);

				String input = config.getString(prefix + ".input");
				input = input == null || input.length() == 0 ? null : formatTime(input);
				instance.setInput(input);

				String output = config.getString(prefix + ".output");
				output = output == null || output.length() == 0 ? null : formatTime(output);
				instance.setOutput(output);

				org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();

				Iterator<String> confKeys = config.getKeys(prefix + ".config");
				while (confKeys.hasNext()) {
					String k = confKeys.next();

					String key = k.substring((prefix + ".config.").length());
					String val = config.getString(k);

					if (val != null) {
						conf.set(key, val);
					}
				}
				instance.setConfig(conf);

				
				String inputformat = config.getString(prefix + ".input.format.class");
				Class<InputFormat<Writable, Writable>> inputformatcls = (Class<InputFormat<Writable, Writable>>)Class.forName(inputformat);
				instance.setInputFormat(inputformatcls);
				
				String outputformat = config.getString(prefix + ".output.format.class");
				Class<OutputFormat<Writable, Writable>> outputformatcls = (Class<OutputFormat<Writable, Writable>>) Class.forName(outputformat);
				instance.setOutputFormat(outputformatcls);

				dumpers.add(instance);

				logger.info(instance + " had loaded");
			}
		} catch (ConfigurationException e) {
			logger.error("error", e);
			throw new RuntimeException(e);
		} catch (ClassNotFoundException e1) {
			logger.error("error", e1);
			throw new RuntimeException(e1);
		} catch (InstantiationException e1) {
			logger.error("error", e1);
			throw new RuntimeException(e1);
		} catch (IllegalAccessException e1) {
			logger.error("error", e1);
			throw new RuntimeException(e1);
		}
	}

	public static DUMPContext get(Configuration config) {
		if (instance == null) {
			instance = new DUMPContext(config);
		}
		return instance;
	}

	public static DUMPContext get() {
		if (instance == null) {
			instance = new DUMPContext(null);
		}
		return instance;
	}

	public List<Dumper<Object, Object>> getDumpers() {
		return dumpers;
	}
}
