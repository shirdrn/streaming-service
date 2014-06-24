package org.shirdrn.streaming.utils;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;

import com.google.common.collect.Maps;

public class ThreadPoolUtils {

	private static final NamedThreadFactory DEFAULT_THREAD_FACTORY = new NamedThreadFactory();
	private static final Map<String, ExecutorService> POOLS = Maps.newHashMap();
	
	public static ThreadFactory newThreadFactory() {
		return DEFAULT_THREAD_FACTORY;
	}
	
	public static ThreadFactory newThreadFactory(String name) {
		return new NamedThreadFactory(name);
	}
	
	public synchronized static ExecutorService newFixedThreadPool(int nThreads, String poolName) {
		checkNamedPool(poolName);
		ExecutorService executorService = Executors.newFixedThreadPool(nThreads);
		ThreadPoolExecutor pool = setThreadFActory(poolName, executorService);
		return pool;
	}
	
	public synchronized static ExecutorService newCachedThreadPool(String poolName) {
		checkNamedPool(poolName);
		ExecutorService executorService = Executors.newCachedThreadPool();
		ExecutorService pool = setThreadFActory(poolName, executorService);
		return pool;
	}

	private static ThreadPoolExecutor setThreadFActory(String poolName,
			ExecutorService executorService) {
		ThreadPoolExecutor pool = (ThreadPoolExecutor) executorService;
		ThreadFactory threadFactory = new NamedThreadFactory(poolName);
		pool.setThreadFactory(threadFactory);
		return pool;
	}

	private static void checkNamedPool(String poolName) {
		ExecutorService executorService = POOLS.get(poolName);
		if(executorService != null) {
			throw new RuntimeException("Thread pool existes for: name=" + poolName + 
					", executorService=" + executorService);
		}
	}
	
}
