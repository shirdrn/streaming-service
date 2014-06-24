package org.shirdrn.streaming.agent;

import java.io.File;
import java.io.FilenameFilter;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.shirdrn.streaming.agent.constants.AgentConstants;
import org.shirdrn.streaming.common.Configurable;
import org.shirdrn.streaming.common.LifecycleAware;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class DirectoriesManager implements LifecycleAware, Configurable {

	private static final Log LOG = LogFactory.getLog(DirectoriesManager.class);
	private Configuration agentConf;
	private final Configuration filesConf;
	// Map<type, dir>
	private final Map<Integer, File> typedDirs = Maps.newHashMap();
	// Map<dir, Set<fileName>>
	private final Map<File, Set<String>> diredFileNames = Maps.newHashMap();
	// Map<file, type>
	private final Map<File, Integer> typedFiles = Maps.newHashMap();
	// Map<dir, file pattern>
	private final Map<File, Pattern> dirToFilePattenMapping = Maps.newHashMap();
	// Map<dir, fileinfo>
	Map<File, List<FileInfo>> diredFileInfos = Maps.newHashMap();
	
	public DirectoriesManager(Configuration filesConf) {
		this.filesConf = filesConf;
	}
	
	@Override
	public void start() throws Exception {
		// load file directories
		loadDirs();
	}

	@Override
	public void stop() throws Exception {
		
	}
	
	@Override
	public void setConfig(Configuration conf) {
		this.agentConf = conf;
	}

	private void loadDirs() {
		LOG.info("Load directories and files...");
		Iterator<String> iter = filesConf.getKeys();
		while(iter.hasNext()) {
			String key = iter.next();
			LOG.debug("File key: " + key);
			int type = Integer.parseInt(key);
			if(!typedDirs.containsKey(type)) {
				String[] values = filesConf.getStringArray(key);
				if(values.length == 2) {
					String strDir = values[0];
					Pattern p = Pattern.compile(values[1]);
					File dir = new File(strDir);
					Preconditions.checkArgument(dir.exists(), "Directory not found: " + dir);
					typedDirs.put(type, dir);
					dirToFilePattenMapping.put(dir, p);
					
					String[] files = listFileNamesInDir(dir);
					LOG.debug("List fileNames: " + Arrays.asList(files));
					for(String fileName : files) {
						Set<String> fileNames = diredFileNames.get(dir);
						if(fileNames == null) {
							// set MUST be synchronized
							fileNames = Collections.synchronizedSet( new HashSet<String>());
							diredFileNames.put(dir, fileNames);
						}
						fileNames.add(fileName);
						LOG.info("  Add: dir=" + dir + ", fileName=" + fileName);
						File file = new File(dir, fileName);
						typedFiles.put(file, type);
						
						// collect dired file infos
						collectDiredFileInfos(type, dir, fileName, file);
					}
				} else {
					LOG.error("Configure error for: " + filesConf);
					throw new RuntimeException("Configure error for file: " 
							+ AgentConstants.AGENT_FILE_CONFIG);
				}
			}
		}
		LOG.debug("Loaded: typedDirs=" + typedDirs);
		LOG.debug("Loaded: typedFiles=" + typedFiles);
		LOG.debug("Loaded: dirToFilePattenMapping=" + dirToFilePattenMapping);
		LOG.debug("Loaded: diredFileInfos=" + diredFileInfos);
		LOG.info("Loaded.");
	}

	private void collectDiredFileInfos(int type, File dir, String fileName,
			File file) {
		FileInfo fi = new FileInfo(fileName, dir, file, type);
		List<FileInfo> fileInfos = diredFileInfos.get(dir);
		if(fileInfos == null) {
			fileInfos = Lists.newArrayList();
			diredFileInfos.put(dir, fileInfos);
		}
		fileInfos.add(fi);
	}

	public String[] listFileNamesInDir(File dir) {
		String[] files = dir.list(new FilenameFilter() {

			@Override
			public boolean accept(File dir, String name) {
				return dirToFilePattenMapping.get(dir).matcher(name).matches();
			}
			
		});
		return files;
	}
	
	@Override
	public Configuration getConfig() {
		return this.agentConf;
	}
	
	public Set<Integer> getTypes() {
		return typedDirs.keySet();
	}
	
	public File getDir(int type) {
		return typedDirs.get(type);
	}
	
	public int getType(File file) {
		return typedFiles.get(file);
	}
	
	public Set<File> getFiles(int type) {
		Set<File> files = new HashSet<File>();
		File dir = typedDirs.get(type);
		for(String fileName : diredFileNames.get(dir)) {
			files.add(new File(dir, fileName));
		}
		return files;
	}
	
	public Set<String> getLogFileNames(File dir) {
		return diredFileNames.get(dir);
	}

	/**
	 * Each directory is mapped to its contained files.  
	 * @return
	 */
	public Map<File, List<FileInfo>> getFiles() {
		return Collections.unmodifiableMap(diredFileInfos);
	}
	
	public static class FileInfo {
		
		String fileName;
		File dir;
		File file;
		int type;
		
		public FileInfo(String fileName, File dir, File file, int type) {
			super();
			this.fileName = fileName;
			this.dir = dir;
			this.file = file;
			this.type = type;
		}
		public String getFileName() {
			return fileName;
		}
		public void setFileName(String fileName) {
			this.fileName = fileName;
		}
		public File getDir() {
			return dir;
		}
		public void setDir(File dir) {
			this.dir = dir;
		}
		public File getFile() {
			return file;
		}
		public void setFile(File file) {
			this.file = file;
		}
		public int getType() {
			return type;
		}
		public void setType(int type) {
			this.type = type;
		}
		@Override
		public int hashCode() {
			int prime = 31;
			return fileName.hashCode() * prime + 
					dir.hashCode() * prime + file.hashCode() * prime + type * prime;
		}
		@Override
		public boolean equals(Object obj) {
			FileInfo other = (FileInfo) obj;
			return this.hashCode() == other.hashCode();
		}
		@Override
		public String toString() {
			StringBuffer sb = new StringBuffer();
			sb.append("[fileName=" + fileName + ", ")
			.append("type=" + type + ", ")
			.append("dir=" + dir + ", ")
			.append("file=" + file + "]");
			return sb.toString();
		}
	}

}
