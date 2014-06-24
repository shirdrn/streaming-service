package org.shirdrn.streaming.common;

import java.net.InetSocketAddress;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.shirdrn.streaming.common.constants.StreamingConstants;
import org.shirdrn.streaming.common.constants.StreamingKeys;

/**
 * Streaming endpoint abstraction.
 * 
 * @author yanjun
 */
public abstract class StreamingEndpoint implements LifecycleAware {

	private static final Log LOG = LogFactory.getLog(StreamingEndpoint.class);
	private final Configuration config;
	private final String host;
	private final int port;
	private final InetSocketAddress socketAddress;
	
	public StreamingEndpoint() throws Exception {
		config = new PropertiesConfiguration(StreamingConstants.DEFAULE_CONFIG);
		host = config.getString(StreamingKeys.STREAMING_SERVER_HOST, "localhost");
		port = config.getInt(StreamingKeys.STREAMING_SERVER_PORT, 8196);
		LOG.info("Configure: host=" + host + ", port=" + port);
		socketAddress = new InetSocketAddress(host, port);
		// add shutdown hook
		addShutdownHook(new Runnable() {
			@Override
			public void run() {
				try {
					stop();
				} catch (Exception e) {
					LOG.warn("Fail to stop this endpoint: ", e);
				}				
			}
		});
	}
	
	public InetSocketAddress getSocketAddress() {
		return socketAddress;
	}
	
	protected void addShutdownHook(Runnable hook) {
		Runtime.getRuntime().addShutdownHook(new Thread(hook));
	}
}
