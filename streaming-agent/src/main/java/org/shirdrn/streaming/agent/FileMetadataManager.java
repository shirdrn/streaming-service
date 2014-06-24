package org.shirdrn.streaming.agent;

import java.io.Closeable;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CodingErrorAction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.shirdrn.streaming.agent.DirectoriesManager.FileInfo;
import org.shirdrn.streaming.agent.FileReaderManager.ReadTask;
import org.shirdrn.streaming.agent.constants.AgentConstants;
import org.shirdrn.streaming.agent.constants.AgentKeys;
import org.shirdrn.streaming.common.FileMeta;
import org.shirdrn.streaming.common.LifecycleAware;
import org.shirdrn.streaming.utils.Pair;
import org.shirdrn.streaming.utils.ThreadPoolUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

public class FileMetadataManager implements LifecycleAware {

	private static final Log LOG = LogFactory.getLog(FileMetadataManager.class);
	private final Configuration agentConfig;
	private final DirectoriesManager directoriesManager;
	private final File metadataRoot;
	
	private static ThreadLocal<CharsetEncoder> ENCODER_FACTORY = new ThreadLocal<CharsetEncoder>() {
		@Override
		protected CharsetEncoder initialValue() {
			return Charset.forName("UTF-8").newEncoder()
					.onMalformedInput(CodingErrorAction.REPLACE)
					.onUnmappableCharacter(CodingErrorAction.REPLACE);
		}
	};

	private static ThreadLocal<CharsetDecoder> DECODER_FACTORY = new ThreadLocal<CharsetDecoder>() {
		@Override
		protected CharsetDecoder initialValue() {
			return Charset.forName("UTF-8").newDecoder()
					.onMalformedInput(CodingErrorAction.REPLACE)
					.onUnmappableCharacter(CodingErrorAction.REPLACE);
		}
	};
	
	private ExecutorService executorService;
	private LogFileIdxWriter idxMetaWriter;
//	private LogTxMetaWriter txMetaWriter;
	private Map<Integer, LogTxMetaWriter> txMetaWriters = Maps.newHashMap();
	// Map<fileID, file>
	private final ConcurrentMap<Integer, File> idToFileIndexMap = Maps.newConcurrentMap();
	// Map<file, fileID>
	private final ConcurrentMap<File, Integer> fileToIdIndexMap = Maps.newConcurrentMap();
	// Map<fileID, txid>
	private final Map<Integer, Long> fileIDToTxidMap = Maps.newHashMap();
	// After succeeding to send a event, event will be in the q
	private final Map<Integer, BlockingDeque<FileMeta>> txCallbackQueues = Maps.newHashMap();
	// Map<type, lastTxMetaFile>
	private Map<Integer, File> lastTxMetaFileMap = Maps.newHashMap();
	private final int qOfferOrPollWaitInterval;
	
	public FileMetadataManager(DirectoriesManager directoriesManager) {
		super();
		Preconditions.checkArgument(directoriesManager != null, "directoriesManager==null");
		this.directoriesManager = directoriesManager;
		agentConfig = directoriesManager.getConfig();
		qOfferOrPollWaitInterval = agentConfig.getInt(
				AgentKeys.AGENT_MESSAGE_QUEUE_OFFER_OR_POLL_WAIT_INTERVAL, 200);
		
		metadataRoot = checkMetaRoot(agentConfig);
		final File idxMetaFile = getIdxMetaFile();
		// if idx file is empty, delete it
		if(idxMetaFile.exists() && idxMetaFile.length() == 0) {
			Preconditions.checkArgument(getIdxMetaFile().delete(), "Fail to delete empty IDX file!");
		}
		LOG.info("IDX meta file: " + idxMetaFile);
		
		try {
			idxMetaWriter = new LogFileIdxWriter(idxMetaFile);
			
			// each log type has a tx meta writer
			for(int type : directoriesManager.getTypes()) {
				String logFileName = directoriesManager.getFiles(type).iterator().next().getName();
				File txMetaFile = getTxMetaFile(logFileName);
				LOG.info("TX meta file: " + txMetaFile);
				if(txMetaFile.exists() && txMetaFile.length()>0) {
					File lastTxMetaFile = renameTxFile(txMetaFile, logFileName);
					// add to lastTxMetaFileMap
					lastTxMetaFileMap.put(type, lastTxMetaFile);
					txMetaFile = getTxMetaFile(logFileName);
				}
				LogTxMetaWriter txMetaWriter = new LogTxMetaWriter(txMetaFile);
				txMetaWriters.put(type, txMetaWriter);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private File getTxMetaFile(String logFileName) {
		return new File(metadataRoot, logFileName + AgentConstants.STREAMING_META_TX_SUFFIX);
	}
	
	private File getIdxMetaFile() {
		return new File(metadataRoot, AgentConstants.STREAMING_META_IDX);
	}
	
	private File renameTxFile(File txMetaFile, String fileName) {
		File f = txMetaFile;
		final Pattern pattern = 
				Pattern.compile(fileName + "\\" + AgentConstants.STREAMING_META_TX_SUFFIX + "\\.\\d+");
		String[] txFilenames = metadataRoot.list(new FilenameFilter() {
			@Override
			public boolean accept(File dir, String name) {
				return pattern.matcher(name).matches();
			}
		});
		int maxIdx = getMaxSeqNo(txFilenames);
		if(txMetaFile.exists()) {
			File newFile = new File(metadataRoot, txMetaFile.getName() + "." + (maxIdx + 1));
			txMetaFile.renameTo(newFile);
			f = newFile;
		}
		LOG.info("Rename tx meta file: from=" + txMetaFile + ", to=" + f);
		return f;
	}

	private int getMaxSeqNo(String[] txFilenames) {
		int max = -1;
		for(String name : txFilenames) {
			String[] a = name.split("\\.");
			try {
				Integer num = Integer.parseInt(a[a.length-1]);
				if(num > max) {
					max = num;
				}
			} catch (Exception e) {}
		}
		LOG.debug("Get tx meta file max seq num: " + max);
		return max;
	}

	public File getMetaFile(String metaFileName) {
		return new File(metadataRoot, metaFileName);
	}
	
	public static boolean metaExists(Configuration conf) {
		File meta = checkMetaRoot(conf);
		return meta.list().length > 0;
	}

	private static File checkMetaRoot(Configuration conf) {
		String metaDir = conf.getString(AgentKeys.AGENT_METADATA_ROOT_DIR);
		Preconditions.checkArgument(metaDir != null, "Log file meta directory MUST not be null!");
		File meta = new File(metaDir);
		Preconditions.checkArgument(meta.exists(), "Log file meta directory not found!");
		return meta;
	}
	
	@Override
	public void start() throws Exception {
		// load/read idx meta file
		initIdxFile();
		
		// load tx meta files
		for(int type : directoriesManager.getTypes()) {
			initTxMetaFile(type);
			BlockingDeque<FileMeta> deque = new LinkedBlockingDeque<FileMeta>(2000);
			txCallbackQueues.put(type, deque);
		}
				
		executorService = ThreadPoolUtils.newCachedThreadPool("TXCHKER");
		// check the tx queue, and store fileID and tx
		for(int type : directoriesManager.getTypes()) {
			Thread txChecker = new TxCallbackChecker(type);
			executorService.execute(txChecker);
		}
	}
	
	final class TxCallbackChecker extends Thread {
		
		private final BlockingDeque<FileMeta> txDeque;
		private final LogTxMetaWriter txWriter;
		private final int txQueueCheckInterval;
		
		public TxCallbackChecker(int logType) {
			// the interval should be small, cos we should write txid sequences as soon as possible,
			// and the returned callback TX couldn't miss due to unexceptionally application sceneries,
			// such as program crash, machine down, etc.
			txQueueCheckInterval = agentConfig.getInt(
					AgentKeys.AGENT_CALLBACK_TX_QUEUE_CHECK_INTERVAL, 200);
			txDeque = txCallbackQueues.get(logType);
			Preconditions.checkArgument(txDeque != null, "Coundn't find TX deque for: logType=" + logType);
			
			txWriter = txMetaWriters.get(logType);
			Preconditions.checkArgument(txWriter != null, "Never intialize TX writer for: logType=" + logType);
		}
		@Override
		public void run() {
			while(true) {
				try {
					FileMeta event = txDeque.pollFirst();
					if(event != null) {
						txWriter.write(event);
						txWriter.commit();
						LOG.debug("Sync: event=[" + event + "]");
						LOG.info("Sync: txid=" + event.getTxid());
						// add to memory storage
						fileIDToTxidMap.put(event.getFileId(), event.getTxid());
					} else {
						Thread.sleep(txQueueCheckInterval);
						LOG.debug("Tx Q is empty, wait " + txQueueCheckInterval + " ms...");
					}
				} catch (Exception e) {
					LOG.warn("", e);
				}
			}
		}
	}

	private void initTxMetaFile(int type) throws IOException {
		// if meta data exists
		if(metaExists(agentConfig)) {
			// open meta writer
			LogTxMetaWriter txWriter = txMetaWriters.get(type);
			txWriter.open();
			while(txWriter.hasNext()) {
				FileMeta event = txWriter.next();
				fileIDToTxidMap.put(event.getFileId(), event.getTxid());
			}
		}
	}
	
	public ReadTask newReadTask(int logType, int fileID, int offset) {
		ReadTask task = new ReadTask();
		FileMeta meta = new FileMeta();
		meta.setType(logType);
		meta.setFileId(fileID);
		meta.setOffset(offset);
		meta.setTxid(TxidGenerator.next());
		task.type = logType;
		task.meta = meta;
		task.file = idToFileIndexMap.get(fileID);
		return task;
	}
	
	public int newFileID(File file) {
		int fileID = FileIDGenerator.next();
		try {
			idxMetaWriter.write(Pair.of(fileID, file.getAbsolutePath()));
			LOG.info("Written: fileID=" + fileID + ", file=" + file);
			putFileIndex(fileID, file);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return fileID;
	}
	
	private void putFileIndex(int fileID, File file) {
		idToFileIndexMap.put(fileID, file);
		fileToIdIndexMap.put(file, fileID);
	}

	/**
	 * If file 'log.idx' exists, load file data into memory, and merge with files
	 * in the configured directory currently, else write file meta data into
	 * file 'log.idx'.
	 * @throws IOException 
	 */
	private void initIdxFile() throws IOException {
		// if meta data doesn't exist
		idxMetaWriter.open(); // open idx meta writer
		if(!metaExists(agentConfig)) {
			LOG.info("Meta files not found, first time initialize...");
			// write file index data
			// save file
			saveOrMergeIdxFiles();
			for(Entry<Integer, File> pair : idToFileIndexMap.entrySet()) {
				int fileID = pair.getKey();
				String file = pair.getValue().getAbsolutePath();
				LOG.debug("Write file index: fileID=" + fileID + ", file=" + file);
				idxMetaWriter.write(Pair.of(fileID, file));
			}
			idxMetaWriter.commit();
		} else {
			// read data from file index
			while(idxMetaWriter.hasNext()) {
				Pair<Integer, String> pair = idxMetaWriter.next();
				int fileID = pair.getLeft();
				File file = new File(pair.getRight());
				LOG.debug("Read file index: fileID=" + fileID + ", file=" + file);
				putFileIndex(fileID, file);
			}
			// set fileID generator start ID
			setStartFileIDForGenerator();
			
			// merge log files
			saveOrMergeIdxFiles();
		}
		// Close idx meta writer.
		idxMetaWriter.close();
		
	}

	/**
	 * Wait {@link FileReaderManager} instance to pull
	 * the read tasks after {@link FileReaderManager} is started.
	 * @throws IOException 
	 */
	public List<ReadTask> pullInitialReadTasks() throws IOException {
		List<ReadTask> tasks = new ArrayList<ReadTask>();
		// first time start
		if(lastTxMetaFileMap.isEmpty()) {
			LOG.debug("First time to start, initialize read tasks...");
			for(Map.Entry<Integer,File> entry : idToFileIndexMap.entrySet()) {
				collectReadTasks(tasks, entry.getKey(), 0, entry.getValue());
			}
		} else {
			// after recover, seek the proper point to read
			LOG.debug("Try to recover tx files, and reset read pos...");
			for(Entry<Integer, File> entry : lastTxMetaFileMap.entrySet()) {
				File lastTxMetaFile = entry.getValue();
				LogTxMetaWriter reader = new LogTxMetaWriter(lastTxMetaFile);
				reader.open("r");
				// <fileID, max offset>
				Map<Integer, AtomicInteger> m = new HashMap<Integer, AtomicInteger>();
				while(reader.hasNext()) {
					FileMeta meta = reader.next();
					AtomicInteger value = m.get(meta.getFileId());
					if(value == null) {
						value = new AtomicInteger(0);
						m.put(meta.getFileId(), value);
					}
					if(value.get() < meta.getOffset()) {
						value.set(meta.getOffset());
					}
				}
				LOG.debug("Tx meta map: " + m);
				for(Entry<Integer, AtomicInteger> mEntry : m.entrySet()) {
					File file = idToFileIndexMap.get(mEntry.getKey());
					// offset points to the next unread line
					collectReadTasks(tasks, mEntry.getKey(), mEntry.getValue().get() + 1, file);
				}
			}
		}
		LOG.debug("Initially collected tasks: " + tasks.size());
		return tasks;
	}
	
	private void collectReadTasks(List<ReadTask> tasks, int fileID, int offset, File file) {
		int type = directoriesManager.getType(file);
		LOG.debug("Log file need to read: type=" + type + ", fileID=" + fileID + ", file=" + file);
		ReadTask task = newReadTask(type, fileID, offset);
		tasks.add(task);
	}

	private void saveOrMergeIdxFiles() {
		LOG.debug("Prepare to iterate log files: " + directoriesManager.getFiles());
		for(Entry<File,List<FileInfo>> entry : directoriesManager.getFiles().entrySet()) {
			for(FileInfo fi : entry.getValue()) {
				if(!fileToIdIndexMap.containsKey(fi.getFile())) {
					LOG.debug("New file ID for: dir=" + entry.getKey() + ", file=" + fi.getFile());
					newFileID(fi.getFile());
				}
			}
		}
	}

	private void setStartFileIDForGenerator() {
		int max = 0;
		for(int id : idToFileIndexMap.keySet()) {
			if(id > max) {
				max = id;
			}
		}
		FileIDGenerator.setInitialValue(max);
	}
	
	interface Readable<T> {
		
		boolean hasNext() throws IOException;
		T next() throws IOException;
	}
	
	final class LogFileIdxWriter extends AbstractWriter<Pair<Integer, String>> 
		implements Readable<Pair<Integer, String>> {
		
		LogFileIdxWriter(File file) throws IOException {
			super(file);
		}
		
		@Override
		public void write(Pair<Integer, String> event) throws IOException {
			Preconditions.checkArgument(fileHandle.getChannel().isOpen(), "File was closed!");
			// write fileID
			fileHandle.writeInt(event.getLeft());
			CharsetEncoder encoder = ENCODER_FACTORY.get();
			ByteBuffer buf = encoder.encode(CharBuffer.wrap(event.getRight().toCharArray()));
			// write file name length
			fileHandle.writeInt(buf.limit());
			// write file
			fileHandle.write(buf.array(), 0, buf.limit());
		}

		@Override
		public Pair<Integer, String> next() throws IOException {
			int fileID = fileHandle.readInt();
			int len = fileHandle.readInt();
			CharsetDecoder decoder = DECODER_FACTORY.get();
			byte[] buf = new byte[len];
			fileHandle.read(buf, 0, len);
			String file = decoder.decode(ByteBuffer.wrap(buf)).toString();
			return Pair.of(fileID, file);
		}

		@Override
		public boolean hasNext() throws IOException {
			return super.getPosition() < fileHandle.length();
		}
	}
	
	final class LogTxMetaWriter extends AbstractWriter<FileMeta> 
		implements Readable<FileMeta> {
		
		public LogTxMetaWriter(File file) throws IOException {
			super(file);
		}
		
		@Override
		public void write(FileMeta event) throws IOException {
			// write logType
			fileHandle.writeInt(event.getType());
			// write fileID
			fileHandle.writeInt(event.getFileId());
			// write offset
			fileHandle.writeInt(event.getOffset());
			// write txid
			fileHandle.writeLong(event.getTxid());
		}
		
		@Override
		public boolean hasNext() throws IOException {
			return super.getPosition() < fileHandle.length();
		}
		
		@Override
		public FileMeta next() throws IOException {
			int logType = fileHandle.readInt();
			int fileID = fileHandle.readInt();
			int offset = fileHandle.readInt();
			long txid = fileHandle.readLong();
			LOG.debug("logType=" + logType + ", fileID=" + fileID + ", offset=" + offset + ", txid=" + txid);
			return FileMeta.from(logType, fileID, offset, txid);
		}
	}

	abstract class FileOp implements Closeable {
		
		protected final File file;
		protected RandomAccessFile fileHandle;
		
		public FileOp(File file) {
			this.file = file;
			
		}
		
		protected long getPosition() throws IOException {
			return fileHandle.getFilePointer();
		}
		
		public void open(String mode) throws IOException {
			this.fileHandle = new RandomAccessFile(this.file, mode);
		}
		
		@Override
		public void close() throws IOException {
			fileHandle.close();
		}
	}
	
	abstract class AbstractWriter<T> extends FileOp {
		
		public AbstractWriter(File file) throws IOException {
			super(file);
		}
		
		public void open() throws IOException {
			super.open("rw");
		}
		
		public abstract void write(T event) throws IOException;
		
		public void commit() throws IOException {
			fileHandle.getChannel().force(false);
		}
	}
	

	public DirectoriesManager getDirectoriesManager() {
		return directoriesManager;
	}

	@Override
	public void stop() throws Exception {
		idxMetaWriter.close();
		for(Entry<Integer, LogTxMetaWriter> entry : txMetaWriters.entrySet()) {
			entry.getValue().close();	
		}
	}
	
	static final class TxidGenerator {

		private TxidGenerator() {
		}

		private static final AtomicLong TXID = new AtomicLong(System.currentTimeMillis());

		public static void setSeed(long highest) {
			long previous;
			while (highest > (previous = TXID.get())) {
				TXID.compareAndSet(previous, highest);
			}
		}

		public static long next() {
			return TXID.incrementAndGet();
		}
	}
	
	
	static final class FileIDGenerator {

		private FileIDGenerator() {
		}

		private static final AtomicInteger ID = new AtomicInteger(0);

		public static void setInitialValue(int value) {
			ID.set(value);
		}
		public static int next() {
			return ID.incrementAndGet();
		}
	}
	
	public void completeTx(FileMeta event) {
		BlockingDeque<FileMeta> q = txCallbackQueues.get(event.getType());
		while(!q.offerLast(event)) {
			try {
				Thread.sleep(qOfferOrPollWaitInterval);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
}
