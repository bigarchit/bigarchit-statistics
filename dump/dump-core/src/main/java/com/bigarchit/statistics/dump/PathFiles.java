package com.bigarchit.statistics.dump;

import java.util.List;

public class PathFiles {
	public String path;
	public String pathfix;
	public List<String> filenames;

	public PathFiles(String path, String pathfix, List<String> filenames) {
		super();
		this.path = path;
		this.pathfix = pathfix;
		this.filenames = filenames;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((filenames == null) ? 0 : filenames.hashCode());
		result = prime * result + ((path == null) ? 0 : path.hashCode());
		result = prime * result + ((pathfix == null) ? 0 : pathfix.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		PathFiles other = (PathFiles) obj;
		if (filenames == null) {
			if (other.filenames != null)
				return false;
		} else if (!filenames.equals(other.filenames))
			return false;
		if (path == null) {
			if (other.path != null)
				return false;
		} else if (!path.equals(other.path))
			return false;
		if (pathfix == null) {
			if (other.pathfix != null)
				return false;
		} else if (!pathfix.equals(other.pathfix))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "PathFiles [path=" + path + ", pathfix=" + pathfix + ", filenames=" + filenames + "]";
	}

}
