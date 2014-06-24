package org.shirdrn.streaming.server.common;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.configuration.Configuration;

/**
 * Abstract message dispatcher.
 * 
 * @author yanjun
 */
public abstract class AbstractMessageDispatcher<M> implements
		MessageDispatcher<M> {

//	private static final Log LOG = LogFactory.getLog(AbstractMessageDispatcher.class);
	protected final Configuration serverConfig;
	private final ExecutorService handlerWorkerPool;
	
	public AbstractMessageDispatcher(Configuration serverConfig) {
		this.serverConfig = serverConfig;
		handlerWorkerPool = Executors.newCachedThreadPool();
	}

	public ExecutorService getHandlerWorkerPool() {
		return handlerWorkerPool;
	}

	@Override
	public void start() throws Exception {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void stop() throws Exception {
		handlerWorkerPool.shutdown();		
	}
	

}
