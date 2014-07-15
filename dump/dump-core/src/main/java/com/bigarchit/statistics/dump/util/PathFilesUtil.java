package com.bigarchit.statistics.dump.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.log4j.Logger;

import com.bigarchit.statistics.dump.PathFiles;

public class PathFilesUtil {
	
	private static final Logger logger = Logger.getLogger(PathFilesUtil.class);
	
	public static Set<String> clearTags(FileSystem hdfs, final String tagPrefix, Path path) throws Exception {
		Set<String> deletetags = new LinkedHashSet<String>();
		FileStatus[] files = null;
		try {
			files = hdfs.listStatus(path);
		} catch (RemoteException re) {
			throw re;
		} catch (IOException e) {
			throw e;
		}
		if (files == null) {
			return deletetags;
		}
		for (FileStatus file : files) {
			if (file.isDirectory()) {
				deletetags.addAll(clearTags(hdfs, tagPrefix, file.getPath()));
			} else {
				String filename = file.getPath().toString();
				if(filename.substring(filename.lastIndexOf("/")).equals(tagPrefix.toUpperCase() + "_DUMPED")){
					hdfs.deleteOnExit(file.getPath());
					deletetags.add(filename);
				}
			}
		}
		return deletetags;
	}
	

	public static Set<String> createTags(FileSystem hdfs, final String tagPrefix, Path path) throws Exception {
		Set<String> addtags = new LinkedHashSet<String>();
		FileStatus[] files = null;
		try {
			files = hdfs.listStatus(path);
		} catch (RemoteException re) {
			throw re;
		} catch (IOException e) {
			throw e;
		}
		if (files == null) {
			return addtags;
		}
		for (FileStatus file : files) {
			if (file.isDirectory()) {
				addtags.addAll(createTags(hdfs, tagPrefix, file.getPath()));
			} else {
				String filename = file.getPath().toString();

				if (filename.indexOf("temporary") > -1) {
					return addtags;
				}
				if (filename.endsWith("_SUCCESS")) {
					String home = filename.substring(0, filename.length() - 8);
					List<String> fns = listPahFiles(hdfs, path);
					if(!fns.contains(tagPrefix.toUpperCase() + "_DUMPED")){
						FSDataOutputStream out = hdfs.create(new Path(home + "/" + tagPrefix.toUpperCase() + "_DUMPED"), true);
						out.close();
						addtags.add(home);
					}
				} else {
					logger.debug("skip " + filename + "....");
				}
			}
		}
		return addtags;
	}
	
	
	
	
	public static  Set<PathFiles> scanPath(FileSystem hdfs, final String tagPrefix, Path path) throws Exception {
		Set<PathFiles> pathfiles = new LinkedHashSet<PathFiles>();
		FileStatus[] files = null;
		try {
			files = hdfs.listStatus(path);
		} catch (RemoteException re) {
			throw re;
		} catch (IOException e) {
			throw e;
		}
		if (files == null) {
			return pathfiles;
		}

		for (FileStatus file : files) {
			if (file.isDirectory()) {
				pathfiles.addAll(scanPath(hdfs, tagPrefix, file.getPath()));
			} else {
				String filename = file.getPath().toString();

				if (filename.indexOf("temporary") > -1) {
					return pathfiles;
				}

				if (filename.endsWith("_SUCCESS")) {
					String pathfix = filename.substring(filename.indexOf("/", filename.indexOf("://") + 3), filename.length() - 8);
					List<String> fns = listPahFiles(hdfs, path);
					if (!fns.contains(tagPrefix.toUpperCase() + "_DUMPED")) {
						PathFiles pf = new PathFiles(path.toString(), pathfix, fns);
						pathfiles.add(pf);
					}
				} else {
					logger.debug("skip " + filename + "....");
				}
			}
		}
		return pathfiles;
	}
	
	public static List<String> listPahFiles(FileSystem hdfs, Path path) throws Exception{
		final List<String> filenames = new ArrayList<String>();
		FileStatus[] files = null;
		try {
			files = hdfs.listStatus(path);
		} catch (IOException e1) {
			throw e1;
		}
		if (files == null) {
			return new ArrayList<String>();
		}
		for (FileStatus file : files) {
			String filename = file.getPath().getName();
			if (file.getPath().toString().indexOf("temporary") > -1 || filename.endsWith("_SUCCESS")) {
				continue;
			}
			filenames.add(filename);
		}
		return filenames;
	}

}
