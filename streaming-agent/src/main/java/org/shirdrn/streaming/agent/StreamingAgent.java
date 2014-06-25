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
import org.shirdrn.streaming.agent.constants.AgentKeys;
import org.shirdrn.streaming.common.StreamingEndpoint;
import org.shirdrn.streaming.utils.ReflectionUtils;
import org.shirdrn.streaming.utils.ThreadPoolUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

public class StreamingAgent extends StreamingEndpoint {

	private static final Log LOG = LogFactory.getLog(StreamingAgent.class);
	private final Configuration agentConfig;
	private Configuration fileConfig;
	private DirectoriesManager directoriesManager;
	private FileMetadataManager fileMetadataManager;
	private FileReaderManager fileReaderManager;
	private ExecutorService clientExecutorService;
	private final String clientPoolName = "CLI";
	private final Map<PushClient, Boolean> clients = Maps.newHashMap();
	private final String pushClientClazz;
	
	public StreamingAgent() throws Exception {
		super();
		agentConfig = new PropertiesConfiguration(AgentConstants.AGENT_CONFIG);
		fileConfig = new PropertiesConfiguration(AgentConstants.AGENT_FILE_CONFIG);
		pushClientClazz = agentConfig.getString(AgentKeys.AGENT_CLIENT_PUSH_CLIENT_CLASS);
		Preconditions.checkArgument(pushClientClazz != null ,
				"Push client implementation class MUST not be null!");
		LOG.info("Push client implementation class: " + pushClientClazz);
	}

	@Override
	public void start() throws Exception {
		// create & configure directories manager
		directoriesManager = new DirectoriesManager(fileConfig);
		directoriesManager.setConfig(agentConfig);
		directoriesManager.start();
		LOG.info("Log dirs manager started!");
		
		// create & configure file metadata manager
		fileMetadataManager = new FileMetadataManager(directoriesManager);
		fileMetadataManager.start();
		LOG.info("Log file meta manager started!");
		
		// create & configure file reader manager
		fileReaderManager = new FileReaderManager(fileMetadataManager);
		fileReaderManager.setConfig(agentConfig);
		fileReaderManager.start();
		LOG.info("Reader manager started!");
		
		// create & configure streaming clients
		int clientCount = computeClients();
		clientExecutorService = ThreadPoolUtils.newFixedThreadPool(clientCount, clientPoolName);
		configureClient();
	}

	private int computeClients() {
		return directoriesManager.getTypes().size();
	}

	private void configureClient() {
		int clientCount = computeClients();
		LOG.info("Prepare to create " + clientCount + " push clients.");
		final StreamingEndpoint endpoint = this;
		List<Integer> types = new ArrayList<Integer>(directoriesManager.getTypes());
		for(int i=0; i<clientCount; i++) {
			try {
				final int id = i;
				final int type = types.get(id);
				final PushClient c = ReflectionUtils.newInstance(pushClientClazz, 
						PushClient.class, this.getClass().getClassLoader(),
						new Object[] {endpoint, id, type, fileReaderManager});
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
		
		directoriesManager.stop();
		LOG.info("Log dirs manager stopped!");
		
		fileMetadataManager.stop();
		LOG.info("Log file meta manager stopped!");
		
		fileReaderManager.stop();
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
