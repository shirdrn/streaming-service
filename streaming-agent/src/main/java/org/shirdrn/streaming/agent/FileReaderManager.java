package org.shirdrn.streaming.agent;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.shirdrn.streaming.agent.FileMetadataManager.TxidGenerator;
import org.shirdrn.streaming.agent.constants.AgentKeys;
import org.shirdrn.streaming.common.Configurable;
import org.shirdrn.streaming.common.FileLineMessage;
import org.shirdrn.streaming.common.FileMeta;
import org.shirdrn.streaming.common.LifecycleAware;
import org.shirdrn.streaming.common.file.AbstractReader;
import org.shirdrn.streaming.common.file.FileReadable;
import org.shirdrn.streaming.utils.Pair;
import org.shirdrn.streaming.utils.ThreadPoolUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

public class FileReaderManager implements LifecycleAware, Configurable {

	private static final Log LOG = LogFactory.getLog(FileReaderManager.class);
	private Configuration agentConfig;
	private final FileMetadataManager fileMetadataManager;
	private final DirectoriesManager directoriesManager;
	// Map<type, reader>
	private final Map<Integer, ReadWorker> typedReaderThreads = Maps.newHashMap();
	// Event sent by client(PUSH side)
	private final Map<Integer, BlockingDeque<FileLineMessage>> typedMessageQueues = Maps.newHashMap();
	
	private ExecutorService readerExecutorService;
	private final String readerPoolName = "READER";
	
	public FileReaderManager(FileMetadataManager fileMetadataManager) {
		super();
		this.fileMetadataManager = fileMetadataManager;
		this.directoriesManager = fileMetadataManager.getDirectoriesManager();
		agentConfig = directoriesManager.getConfig();
		// create bounded queue
		int capacity = agentConfig.getInt(AgentKeys.AGENT_MESSAGE_QUEUE_CAPACITY, 20000);
		LOG.info("Bounded event queue capacity: " + capacity);
		
		// for each type, has a separated event queue
		for(int type : directoriesManager.getTypes()) {
			BlockingDeque<FileLineMessage> eventQ = new LinkedBlockingDeque<FileLineMessage>(capacity);
			typedMessageQueues.put(type, eventQ);
			LOG.debug("Create eventQ: type=" + type + ", eventQ=" + eventQ);
		}
	}

	@Override
	public void setConfig(Configuration conf) {
		agentConfig = conf;
	}
	
	@Override
	public void start() throws Exception {
		// initialize reader threads
		initializeReaders();
		
		// pull initial read tasks
		List<ReadTask> tasks = fileMetadataManager.pullInitialReadTasks();
		for(ReadTask task : tasks) {
			this.addReadTask(task);
		}
		
		// start reader thread
		startReaderThreads();
	}
	
	private void initializeReaders() {
		LOG.info("Initialize readers...");
		int nReaders = directoriesManager.getTypes().size();
		readerExecutorService = Executors.newFixedThreadPool(nReaders);
		for(Integer type : directoriesManager.getTypes()) {
			ReadWorker worker = new ReadWorker(type);
			typedReaderThreads.put(type, worker);
			LOG.debug("Initialize reader thread: type=" + type + ", thread=" + worker);
		}
		LOG.info("Initialized.");
	}
	
	private void startReaderThreads() {
		LOG.info("Reader threads are starting...");
		int nReaders = directoriesManager.getTypes().size();
		readerExecutorService = ThreadPoolUtils.newFixedThreadPool(nReaders, readerPoolName);
		for(Entry<Integer, ReadWorker> entry : typedReaderThreads.entrySet()) {
			LOG.info("Push reader thread to pool: type=" + entry.getKey() + ", thread=" + entry.getValue());
			readerExecutorService.execute(entry.getValue());
		}
		LOG.info("All reader threads are started.");
	}

	@Override
	public void stop() throws Exception {
		readerExecutorService.shutdown();
	}
	
	static class ReadTask {
		
		protected int type;
		protected FileMeta meta;
		protected File file;
		
		@Override
		public String toString() {
			return "type=" + type + ", file=" + file;
		}
	}
	
	public void addReadTask(ReadTask task) {
		LOG.debug("Received read task: " + task);
		ReadWorker t = typedReaderThreads.get(task.type);
		LOG.debug("Assign read task to: " + t);
		if(t != null) {
			try {
				t.readTaskQueue.put(task);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
	final class ReadWorker implements Runnable {

		private final int type;
		private final File dir;
		private final BlockingDeque<ReadTask> readTaskQueue ;
		private final  BlockingDeque<FileLineMessage> eventQ;
		private final int waitFileBeingWrittenInterval;
		private final int waitReadTaskArrivalInterval;
		private FileReadable<Pair<Integer, String>> reader;
		
		public ReadWorker(int type) {
			this.type = type;
			dir = directoriesManager.getDir(type);
			readTaskQueue = new LinkedBlockingDeque<ReadTask>();
			eventQ = typedMessageQueues.get(type);
			Preconditions.checkArgument(eventQ != null, "Coundn't get event queue for: type=" + type);
			
			waitFileBeingWrittenInterval = agentConfig.getInt(
					AgentKeys.AGENT_READER_WORKER_WAIT_FILE_BEING_WRITTEN_INTERVAL, 5000);
			waitReadTaskArrivalInterval = agentConfig.getInt(
					AgentKeys.AGENT_READER_WORKER_WAIT_READ_TASK_ARRIVAL_INTERVAL, 1000);
		}
		
		@Override
		public void run() {
			// used for debugging purpose
			int debugSleepPutToQInterval = 3000;
			// start to read
			LOG.debug("Loop: start to read...");
			ReadTask task = null;
			try {
				// couldn't support to add ReadTask dynamically
				task = readTaskQueue.pollFirst();
				LOG.debug("poll: task=" + task);
				if(task != null) {
					reader = new FileReader(task.file, task.meta);
					reader.open();
					while(true) {
						while(reader.hasNext()) {
							try {
								Pair<Integer, String> line = reader.next();
								FileLineMessage event = new FileLineMessage();
								event.setType(type);
								event.setLine(line.getRight());
								long txid = TxidGenerator.next();
								FileMeta meta = FileMeta.from(
										task.meta.getType(), task.meta.getFileId(), line.getLeft(), txid);
								event.setFileMeta(meta);
								LOG.debug("Read file line: " + line);
								if(LOG.isDebugEnabled()) {
									Thread.sleep(debugSleepPutToQInterval);
								}
								
								// blocked if queue is full
								eventQ.putLast(event);
								LOG.debug("Added to event Q: size=" + eventQ.size() + ", event=[" + event + "]");
							} catch (Exception e) {
								LOG.error("Fail to read a line: ", e);
								if(task != null) {
									readTaskQueue.put(task);
								}
							}
						}
						// reach log file end, or no file line is written
						LOG.debug("wait file being written: " + waitFileBeingWrittenInterval + " ms...");
						Thread.sleep(waitFileBeingWrittenInterval);
					}
				} else {
					Thread.sleep(waitReadTaskArrivalInterval);
				}
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				try {
					if(reader != null) {
						reader.close();
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
				
			}
		}

		@Override
		public String toString() {
			return "READER-TREAHD[" + type + "=>" + dir + "]";
		}
	}
		
	@Override
	public Configuration getConfig() {
		return agentConfig;
	}
	
	final class FileReader extends AbstractReader<Pair<Integer, String>> {

		private final FileMeta fileMeta;
		private long length;
		private long position;
		private int offset;
		private String currentLine = null;

		public FileReader(File file, FileMeta fileMeta) {
			super(file);
			this.fileMeta = fileMeta;
		}
		
		@Override
		public void open() throws IOException {
			super.open();
			LOG.debug("File reader opened: " + randomAccessFile);
			this.length = getLength();
			this.position = getPosition();
			// skip offset lines
			// and seek the proper position to continue to read
			seek();			
		}

		private void seek() throws IOException {
			int i = 0;
			LOG.debug("Line offset: " + fileMeta.getOffset());
			while(i<fileMeta.getOffset()) {
				read();
				i++;
			}
			offset = fileMeta.getOffset();
		}
		
		private void read() throws IOException {
			currentLine = randomAccessFile.readLine();
			currentLine = new String(currentLine.getBytes("ISO8859-1"), "UTF-8");
			LOG.debug("currentLine=" + currentLine);
			position = getPosition();
			++offset;
		}

		@Override
		public boolean hasNext() throws IOException {
			currentLine = null;
			try {
				while(true) {
					LOG.debug("currentPos=" + position + ", length=" + length);
					if(position < length) {
						// here blocking may occur
						read();
						return true;
					} else {
						// check whether file is being written now
						// and then update length
						long len = getLength();
						LOG.debug("Try to update file length: len=" + len);
						if(len > length) {
							length = len;
							continue;
						}
						return false;
					}
				}
			} catch (IOException e) {
				return false;
			}
		}

		@Override
		public Pair<Integer, String> next() throws IOException {
			LOG.debug("Line offset=" + (offset-1));
			return new Pair<Integer, String>(offset-1, currentLine);
		}
		
	}

	public FileMetadataManager getFileMetadataManager() {
		return fileMetadataManager;
	}

	public BlockingDeque<FileLineMessage> getTypedMessageQueue(int type) {
		return typedMessageQueues.get(type);
	}

}
