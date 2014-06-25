package org.shirdrn.streaming.agent;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
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
import org.shirdrn.streaming.file.AbstractWriter;
import org.shirdrn.streaming.file.FileAccessor;
import org.shirdrn.streaming.file.FileReadable;
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
	private FileAccessor<Pair<Integer, String>> idxFileAccessor;
	private Map<Integer, TxFileAccessor> txFileAccessors = Maps.newHashMap();
	// Map<fileId, file>
	private final ConcurrentMap<Integer, File> idToFileIndexMap = Maps.newConcurrentMap();
	// Map<file, fileId>
	private final ConcurrentMap<File, Integer> fileToIdIndexMap = Maps.newConcurrentMap();
	// Map<fileId, txid>
	private final Map<Integer, Long> fileIDToTxidMap = Maps.newHashMap();
	// After succeeding to send a message, message will be in the queue
	private final Map<Integer, BlockingDeque<FileMeta>> txidCallbackQueues = Maps.newHashMap();
	// Map<type, lastTxFile>
	private Map<Integer, File> lastTxFileMap = Maps.newHashMap();
	private final int qOfferOrPollWaitInterval;
	
	public FileMetadataManager(DirectoriesManager directoriesManager) {
		super();
		Preconditions.checkArgument(directoriesManager != null, "directoriesManager==null");
		this.directoriesManager = directoriesManager;
		agentConfig = directoriesManager.getConfig();
		qOfferOrPollWaitInterval = agentConfig.getInt(
				AgentKeys.AGENT_MESSAGE_QUEUE_OFFER_OR_POLL_WAIT_INTERVAL, 200);
		
		metadataRoot = checkMetaRoot(agentConfig);
		final File idxFile = getIdxMetaFile();
		// if idx file is empty, delete it
		if(idxFile.exists() && idxFile.length() == 0) {
			Preconditions.checkArgument(getIdxMetaFile().delete(), "Fail to delete empty idx file!");
		}
		LOG.info("IDX meta file: " + idxFile);
		
		try {
			idxFileAccessor = new IdexFileAccessor(idxFile);
			// each type has a TX file accessor
			for(int type : directoriesManager.getTypes()) {
				String fileName = directoriesManager.getFiles(type).iterator().next().getName();
				File txFile = getTxFile(fileName);
				LOG.info("TX file: " + txFile);
				if(txFile.exists() && txFile.length()>0) {
					File lastTxFile = renameTxFile(txFile, fileName);
					// add to lastFileMap
					lastTxFileMap.put(type, lastTxFile);
					txFile = getTxFile(fileName);
				}
				TxFileAccessor txFileAccessor = new TxFileAccessor(txFile);
				txFileAccessors.put(type, txFileAccessor);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private File getTxFile(String fileName) {
		return new File(metadataRoot, fileName + AgentConstants.STREAMING_META_TX_SUFFIX);
	}
	
	private File getIdxMetaFile() {
		return new File(metadataRoot, AgentConstants.STREAMING_META_IDX);
	}
	
	private File renameTxFile(File txFile, String fileName) {
		File f = txFile;
		final Pattern pattern = 
				Pattern.compile(fileName + "\\" + AgentConstants.STREAMING_META_TX_SUFFIX + "\\.\\d+");
		String[] txFilenames = metadataRoot.list(new FilenameFilter() {
			@Override
			public boolean accept(File dir, String name) {
				return pattern.matcher(name).matches();
			}
		});
		int maxIdx = getMaxSeqNo(txFilenames);
		if(txFile.exists()) {
			File newFile = new File(metadataRoot, txFile.getName() + "." + (maxIdx + 1));
			txFile.renameTo(newFile);
			f = newFile;
		}
		LOG.info("Rename TX file: from=" + txFile + ", to=" + f);
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
		LOG.debug("Get TX file max seq num: " + max);
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
		// load/read IDX file
		initIdxFile();
		
		// load TX files
		for(int type : directoriesManager.getTypes()) {
			initTxMetaFile(type);
			BlockingDeque<FileMeta> deque = new LinkedBlockingDeque<FileMeta>(2000);
			txidCallbackQueues.put(type, deque);
		}
				
		executorService = ThreadPoolUtils.newCachedThreadPool("TXCHKER");
		// check the txid queue, and store fileId and txid
		for(int type : directoriesManager.getTypes()) {
			executorService.execute(new TxCallbackChecker(type));
		}
	}
	
	final class TxCallbackChecker extends Thread {
		
		private final BlockingDeque<FileMeta> txidDeque;
		private final TxFileAccessor txFileAccessor;
		private final int txidDequeCheckInterval;
		
		public TxCallbackChecker(int type) {
			// the interval should be small, cos we should write txid sequences as soon as possible,
			// and the returned callback TX couldn't miss due to unexceptionally application sceneries,
			// such as program crash, machine down, etc.
			txidDequeCheckInterval = agentConfig.getInt(
					AgentKeys.AGENT_CALLBACK_TX_QUEUE_CHECK_INTERVAL, 200);
			txidDeque = txidCallbackQueues.get(type);
			Preconditions.checkArgument(txidDeque != null, "Coundn't find TX deque for: type=" + type);
			
			txFileAccessor = txFileAccessors.get(type);
			Preconditions.checkArgument(txFileAccessor != null, "Never intialize TX accessor for: type=" + type);
		}
		@Override
		public void run() {
			while(true) {
				try {
					FileMeta event = txidDeque.pollFirst();
					if(event != null) {
						txFileAccessor.write(event);
						txFileAccessor.flush();
						LOG.debug("Sync: event=[" + event + "]");
						LOG.info("Sync: txid=" + event.getTxid());
						// add to memory storage
						fileIDToTxidMap.put(event.getFileId(), event.getTxid());
					} else {
						Thread.sleep(txidDequeCheckInterval);
						LOG.debug("Tx deque is empty, wait " + txidDequeCheckInterval + " ms...");
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
			TxFileAccessor txFileAccessor = txFileAccessors.get(type);
			txFileAccessor.open();
			while(txFileAccessor.hasNext()) {
				FileMeta fileMeta = txFileAccessor.next();
				fileIDToTxidMap.put(fileMeta.getFileId(), fileMeta.getTxid());
			}
		}
	}
	
	public ReadTask newReadTask(int type, int fileId, int offset) {
		ReadTask task = new ReadTask();
		FileMeta meta = new FileMeta();
		meta.setType(type);
		meta.setFileId(fileId);
		meta.setOffset(offset);
		meta.setTxid(TxidGenerator.next());
		task.type = type;
		task.meta = meta;
		task.file = idToFileIndexMap.get(fileId);
		return task;
	}
	
	public int newFileID(File file) {
		int fileId = FileIDGenerator.next();
		try {
			idxFileAccessor.write(Pair.of(fileId, file.getAbsolutePath()));
			LOG.info("Written: fileId=" + fileId + ", file=" + file);
			putFileIndex(fileId, file);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return fileId;
	}
	
	private void putFileIndex(int fileID, File file) {
		idToFileIndexMap.put(fileID, file);
		fileToIdIndexMap.put(file, fileID);
	}

	/**
	 * If file '*.idx' exists, load file data into memory, and merge with files
	 * in the configured directory currently, else write file meta data into
	 * file '*.idx'.
	 * @throws IOException 
	 */
	private void initIdxFile() throws IOException {
		// if meta data doesn't exist
		idxFileAccessor.open(); // open idx meta writer
		if(!metaExists(agentConfig)) {
			LOG.info("Meta files not found, first time initialize...");
			// write file index data
			// save file
			saveOrMergeIdxFiles();
			for(Entry<Integer, File> pair : idToFileIndexMap.entrySet()) {
				int fileID = pair.getKey();
				String file = pair.getValue().getAbsolutePath();
				LOG.debug("Write file index: fileID=" + fileID + ", file=" + file);
				idxFileAccessor.write(Pair.of(fileID, file));
			}
			idxFileAccessor.flush();
		} else {
			// read data from file index
			while(idxFileAccessor.hasNext()) {
				Pair<Integer, String> pair = idxFileAccessor.next();
				int fileId = pair.getLeft();
				File file = new File(pair.getRight());
				LOG.debug("Read file index: filed=" + fileId + ", file=" + file);
				putFileIndex(fileId, file);
			}
			// set fileID generator start ID
			setStartFileIDForGenerator();
			
			// merge files
			saveOrMergeIdxFiles();
		}
		// Close IDX file accessor.
		idxFileAccessor.close();
		
	}

	/**
	 * Wait {@link FileReaderManager} instance to pull
	 * the read tasks after {@link FileReaderManager} is started.
	 * @throws IOException 
	 */
	public List<ReadTask> pullInitialReadTasks() throws IOException {
		List<ReadTask> tasks = new ArrayList<ReadTask>();
		// first time start
		if(lastTxFileMap.isEmpty()) {
			LOG.debug("First time to start, initialize read tasks...");
			for(Map.Entry<Integer,File> entry : idToFileIndexMap.entrySet()) {
				collectReadTasks(tasks, entry.getKey(), 0, entry.getValue());
			}
		} else {
			// after recover, seek the proper point to read
			LOG.debug("Try to recover tx files, and reset read pos...");
			for(Entry<Integer, File> entry : lastTxFileMap.entrySet()) {
				File lastTxMetaFile = entry.getValue();
				TxFileAccessor reader = new TxFileAccessor(lastTxMetaFile);
				reader.open("r");
				// <fileID, max offset>
				Map<Integer, AtomicInteger> m = new HashMap<Integer, AtomicInteger>();
				while(reader.hasNext()) {
					FileMeta fileMeta = reader.next();
					AtomicInteger value = m.get(fileMeta.getFileId());
					if(value == null) {
						value = new AtomicInteger(0);
						m.put(fileMeta.getFileId(), value);
					}
					if(value.get() < fileMeta.getOffset()) {
						value.set(fileMeta.getOffset());
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
		LOG.debug("Prepare to iterate files: " + directoriesManager.getFiles());
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
	
	final class IdexFileAccessor extends AbstractWriter<Pair<Integer, String>> 
		implements FileAccessor<Pair<Integer, String>> {

		public IdexFileAccessor(File file) {
			super(file);
		}

		@Override
		public void write(Pair<Integer, String> data) throws IOException {
			Preconditions.checkArgument(randomAccessFile.getChannel().isOpen(), "File was closed!");
			// write fileId
			randomAccessFile.writeInt(data.getLeft());
			CharsetEncoder encoder = ENCODER_FACTORY.get();
			ByteBuffer buf = encoder.encode(CharBuffer.wrap(data.getRight().toCharArray()));
			// write file name length
			randomAccessFile.writeInt(buf.limit());
			// write file
			randomAccessFile.write(buf.array(), 0, buf.limit());			
		}

		@Override
		public boolean hasNext() throws IOException {
			return super.getPosition() < randomAccessFile.length();
		}

		@Override
		public Pair<Integer, String> next() throws IOException {
			int fileId = randomAccessFile.readInt();
			int len = randomAccessFile.readInt();
			CharsetDecoder decoder = DECODER_FACTORY.get();
			byte[] buf = new byte[len];
			randomAccessFile.read(buf, 0, len);
			String file = decoder.decode(ByteBuffer.wrap(buf)).toString();
			return Pair.of(fileId, file);
		}
		
	}
	
	final class TxFileAccessor extends AbstractWriter<FileMeta> 
		implements FileReadable<FileMeta> {
		
		public TxFileAccessor(File file) throws IOException {
			super(file);
		}
		
		@Override
		public void write(FileMeta fileMeta) throws IOException {
			// write type
			randomAccessFile.writeInt(fileMeta.getType());
			// write fileID
			randomAccessFile.writeInt(fileMeta.getFileId());
			// write offset
			randomAccessFile.writeInt(fileMeta.getOffset());
			// write txid
			randomAccessFile.writeLong(fileMeta.getTxid());
		}
		
		@Override
		public boolean hasNext() throws IOException {
			return super.getPosition() < randomAccessFile.length();
		}
		
		@Override
		public FileMeta next() throws IOException {
			int type = randomAccessFile.readInt();
			int fileId = randomAccessFile.readInt();
			int offset = randomAccessFile.readInt();
			long txid = randomAccessFile.readLong();
			LOG.debug("type=" + type + ", fileId=" + fileId + ", offset=" + offset + ", txid=" + txid);
			return FileMeta.from(type, fileId, offset, txid);
		}
	}

	public DirectoriesManager getDirectoriesManager() {
		return directoriesManager;
	}

	@Override
	public void stop() throws Exception {
		idxFileAccessor.close();
		for(Entry<Integer, TxFileAccessor> entry : txFileAccessors.entrySet()) {
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
		BlockingDeque<FileMeta> q = txidCallbackQueues.get(event.getType());
		while(!q.offerLast(event)) {
			try {
				Thread.sleep(qOfferOrPollWaitInterval);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
}
