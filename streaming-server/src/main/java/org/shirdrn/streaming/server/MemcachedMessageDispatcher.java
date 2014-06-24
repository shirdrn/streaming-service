package org.shirdrn.streaming.server;

import java.io.IOException;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import net.spy.memcached.AddrUtil;
import net.spy.memcached.BinaryConnectionFactory;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.internal.OperationFuture;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.shirdrn.streaming.common.FileLineMessage;
import org.shirdrn.streaming.server.common.AbstractMessageDispatcher;
import org.shirdrn.streaming.server.constants.ServerConstants;
import org.shirdrn.streaming.server.constants.ServerKeys;
import org.shirdrn.streaming.utils.DateTimeUtils;
import org.shirdrn.streaming.utils.ThreadPoolUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

/**
 * Memcached message dispatcher.</br>
 * Receive messages from MINA server handler, and dispatch them, and then
 * store processed messages to Memcached storage.
 * 
 * @author yanjun
 */
public class MemcachedMessageDispatcher extends AbstractMessageDispatcher<FileLineMessage> {

	private static final Log LOG = LogFactory.getLog(MemcachedMessageDispatcher.class);
	private Configuration typesConfig;
	private final Map<Integer, BlockingDeque<String>> dataLinesMap = Maps.newHashMap() ;
	private final Map<Integer, String> typeToNameMappings = Maps.newHashMap() ;
	private final ExecutorService cacheWorkerPool;
	private final int capacity;
	private final String cacheWrokerPoolName = "CACHE";
	private static final String TRACE_DATETIME_KEY = "_DATETIME_"; 
	
	public MemcachedMessageDispatcher(Configuration config) {
		super(config);
		try {
			typesConfig = new PropertiesConfiguration(ServerConstants.SERVER_TYPES_CONFIG);
			loadTypeToNameMappings();
		} catch (ConfigurationException e) {
			e.printStackTrace();
		}
		
		int nThreads = typeToNameMappings.keySet().size();
		cacheWorkerPool = ThreadPoolUtils.newFixedThreadPool(nThreads, cacheWrokerPoolName);
		capacity = serverConfig.getInt(ServerKeys.SERVER_MESSAGE_QUEUE_CAPACITY, 50000);
		
		// initialize cache workers
		LOG.info("Initialize cache workers...");
		for(int type : typeToNameMappings.keySet()) {
			try {
				BlockingDeque<String> q = new LinkedBlockingDeque<String>(capacity);
				dataLinesMap.put(type, q);
				CacheWorker worker = new CacheWorker(type);
				LOG.debug("Cache worker created: " + worker);
				cacheWorkerPool.execute(worker);
				LOG.info("Added cache worker to thread pool: " + worker);
			} catch (IOException e) {
				LOG.error("Fail to create cache worker: type=" + type, e);
			}
		}
		LOG.info("Initialized.");
	}
	
	private void loadTypeToNameMappings() {
		Iterator<String> iter = typesConfig.getKeys();
		while(iter.hasNext()) {
			String key = iter.next();
			int type = Integer.parseInt(key);
			String name = typesConfig.getString(key, key);
			typeToNameMappings.put(type, name);
			LOG.info("Load <type, name>: <" + key + ", " + name + ">");
		}		
	}

	@Override
	public void dispatch(FileLineMessage message) {
		Integer key = message.getType();
		String value = message.getLine();
		BlockingDeque<String> q = dataLinesMap.get(key);
		try {
			// add to bounded queue
			if(q != null) {
				q.putLast(value);
			} else {
				LOG.warn("Unknown typed message: type=" + message.getType());
			}
		} catch (Exception e) {
			LOG.error("", e);
		}		
	}
	
	final class CacheWorker extends Thread {
		
		private final int type;
		private final String name;
		private final int cacheUpdateInterval;
		
		private final MemcachedClient memcachedClient;
		
		public CacheWorker(final int type) throws IOException {
			this.type = type;
			this.name = typeToNameMappings.get(type);
			cacheUpdateInterval = 
					serverConfig.getInt(ServerKeys.SERVER_CACHE_UPDATE_INTERVAL, 60 * 1000);
			// memcached client
			String[] memcachedServers = serverConfig.getStringArray(ServerKeys.SERVER_MEMCACHED_SERVERS);
			Preconditions.checkArgument(memcachedServers != null, "Do not configure memcached servers!");
			
			StringBuffer servers = new StringBuffer();
			for(String server : memcachedServers) {
				servers.append(server).append(" ");
			}
			LOG.info("Memcached servers: " + servers.toString());
			memcachedClient = new MemcachedClient(new BinaryConnectionFactory(),
					AddrUtil.getAddresses(servers.toString().trim()));

			Runtime.getRuntime().addShutdownHook(new Thread() {
				@Override
				public void run() {
					LOG.info("Close memcached client for cache worker: type=" + type);
					memcachedClient.shutdown();
					LOG.info("Closed!");
				}
			});
		}
		
		@Override
		public void run() {
			// wait 3000 ms
			try {
				Thread.sleep(3000);
			} catch (InterruptedException e1) {
			}
			// working now
			while(true) {
				try {
					BlockingDeque<String> q = dataLinesMap.get(type);
					int itemCount = q.size();
					boolean cacheUpdated = false;
					if(itemCount > 0) {
						cacheUpdated = true;
						// write data to cache
						updateCache(q, itemCount);
					} else {
						int waitTime = 5000;
						LOG.debug("Q unchaged, wait " + waitTime + " ms...");
						Thread.sleep(waitTime);
					}
					
					if(cacheUpdated) {
						LOG.info("Update cache, sleep :" + cacheUpdateInterval + " ms.");
						Thread.sleep(cacheUpdateInterval);
					}
				} catch (Exception e) {
					LOG.warn("Cache worker catches exception: ", e);
				}
			}
		}

		private void updateCache(BlockingDeque<String> q, int itemCount) 
					throws InterruptedException, ExecutionException {
			JSONObject o = new JSONObject();
			int cnt = itemCount;
			JSONArray parentJa = new JSONArray();
			while(cnt>0 && !q.isEmpty()) {
				// remove head and process it
				String line = q.pollFirst();
				if(line != null) {
					parentJa.add(line.getBytes());
				}
				--cnt;
			}
			o.put("data", parentJa);
			o.put("totalRecords", itemCount);
			
			// add a date string to recognize this packet
			String dt = DateTimeUtils.format(new Date(), "yyyy-MM-dd HH:mm:ss");
			o.put(TRACE_DATETIME_KEY, dt);
			
			String value = o.toString();
			OperationFuture<Boolean> future = memcachedClient.set(name, Integer.MAX_VALUE, value);
			if(future.get()) {
				future = memcachedClient.replace(name, Integer.MAX_VALUE, value);
				if(future.get()) {
					LOG.debug("Update cache for: key=" + name + ", values=" + value);
					LOG.info("Update cache for: key=" + name + ", valueLen=" + value.length());
				}
			}
		}
		
		@Override
		public String toString() {
			return "CacheWorker[type=" + type + ", name=" + name + "]";
		}
	}
	
	@Override
	public void stop() throws Exception {
		super.stop();
	}

}
