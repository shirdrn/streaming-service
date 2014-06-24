package org.shirdrn.streaming.agent.common;

import java.util.concurrent.TimeUnit;

import org.apache.commons.configuration.Configuration;
import org.shirdrn.streaming.agent.FileReaderManager;
import org.shirdrn.streaming.agent.constants.AgentKeys;
import org.shirdrn.streaming.common.RetryPolicies;
import org.shirdrn.streaming.common.RetryPolicy;
import org.shirdrn.streaming.common.StreamingEndpoint;

public abstract class AbstractPushClient implements PushClient {

	protected final int id;
	protected final int type;
	protected RetryPolicy retryPolicy;
	protected final int maxRetries;
	protected final int retrySleepTime;
	protected final FileReaderManager fileReaderManager;
	protected final Configuration agentConfig;
	
	public AbstractPushClient(StreamingEndpoint endpoint, int id, int type, 
			FileReaderManager fileReaderManager) throws Exception {
		super();
		this.id = id;
		this.type = type;
		this.fileReaderManager = fileReaderManager;
		this.agentConfig = fileReaderManager.getConfig();
		// choose retry policy
		maxRetries = agentConfig.getInt(AgentKeys.AGENT_CLIENT_CONNECT_RETRY_TIMES, 10);
		retrySleepTime = agentConfig.getInt(AgentKeys.AGENT_CLIENT_CONNECT_RETRY_SLEEP, 5000);
		
		retryPolicy = newRetryPolicy();
	}
	
	private RetryPolicy newRetryPolicy() {
		return RetryPolicies.retryUpToMaximumCountWithProportionalSleep(
				maxRetries, retrySleepTime, TimeUnit.MILLISECONDS);
	}
	
	protected void resetRetryPolicy() {
		newRetryPolicy();
	}
	
	@Override
	public int getId() {
		return id;
	}

}
