package org.shirdrn.streaming.agent;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.shirdrn.streaming.agent.common.PushClient;
import org.shirdrn.streaming.agent.constants.AgentConstants;
import org.shirdrn.streaming.common.StreamingEndpoint;
import org.shirdrn.streaming.utils.ThreadPoolUtils;

import com.google.common.collect.Maps;

public class StreamingAgent extends StreamingEndpoint {

	private static final Log LOG = LogFactory.getLog(StreamingAgent.class);
	private final Configuration agentConfig;
	private Configuration logConfig;
	private DirectoriesManager logDirsManager;
	private FileMetadataManager logFileMetaManager;
	private FileReaderManager logFileReaderManager;
	private ExecutorService clientExecutorService;
	private final String clientPoolName = "CLIENT";
	private final Map<PushClient, Boolean> clients = Maps.newHashMap();
	
	public StreamingAgent() throws Exception {
		super();
		agentConfig = new PropertiesConfiguration(AgentConstants.AGENT_CONFIG);
		logConfig = new PropertiesConfiguration(AgentConstants.AGENT_FILE_CONFIG);
	}

	@Override
	public void start() throws Exception {
		// create & configure log dirs manager
		logDirsManager = new DirectoriesManager(logConfig);
		logDirsManager.setConfig(agentConfig);
		logDirsManager.start();
		LOG.info("Log dirs manager started!");
		
		// create & configure log file meta manager
		logFileMetaManager = new FileMetadataManager(logDirsManager);
		logFileMetaManager.start();
		LOG.info("Log file meta manager started!");
		
		// create & configure reader manager
		logFileReaderManager = new FileReaderManager(logFileMetaManager);
		logFileReaderManager.setConfig(agentConfig);
		logFileReaderManager.start();
		LOG.info("Reader manager started!");
		
		// create & configure streaming clients
		int clientCount = computeClients();
		clientExecutorService = ThreadPoolUtils.newFixedThreadPool(clientCount, clientPoolName);
		configureClient();
	}

	private int computeClients() {
		return logDirsManager.getTypes().size();
	}

	private void configureClient() {
		int clientCount = computeClients();
		LOG.info("Prepare to create " + clientCount + " push clients.");
		final StreamingEndpoint endpoint = this;
		List<Integer> logTypes = new ArrayList<Integer>(logDirsManager.getTypes());
		for(int i=0; i<clientCount; i++) {
			try {
				final int id = i;
				final int logType = logTypes.get(id);
				final MinaPushClient c = new MinaPushClient(endpoint, id, logType, logFileReaderManager);
				clients.put(c, true);
				LOG.debug("Push client created: " + c);
				clientExecutorService.execute(new Runnable() {
					@Override
					public void run() {
						try {
							c.start();
						} catch (Exception e) {
							e.printStackTrace();
						}						
					}
				});
			} catch (Exception e) {
				LOG.error("Fail to start push client: ", e);
			}
		}
	}

	@Override
	public void stop() throws Exception {
		for(Entry<PushClient, Boolean> client : clients.entrySet()) {
			client.getKey().stop();
			LOG.info("Client stopped: client=" + client.getKey());
		}
		
		logDirsManager.stop();
		LOG.info("Log dirs manager stopped!");
		
		logFileMetaManager.stop();
		LOG.info("Log file meta manager stopped!");
		
		logFileReaderManager.stop();
		LOG.info("Reader manager stopped!");
	}
	
	public static void main(String[] args) throws Exception {
		try {
			final StreamingAgent agent = new StreamingAgent();
			agent.start();
		} catch (Exception e) {
			LOG.error("Fail to start streming agent: ", e);
		}
	}

}
