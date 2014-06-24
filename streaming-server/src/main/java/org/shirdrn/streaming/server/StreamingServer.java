package org.shirdrn.streaming.server;

import java.net.InetSocketAddress;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.filter.codec.serialization.ObjectSerializationCodecFactory;
import org.apache.mina.transport.socket.SocketAcceptor;
import org.apache.mina.transport.socket.nio.NioSocketAcceptor;
import org.shirdrn.streaming.common.FileLineMessage;
import org.shirdrn.streaming.common.StreamingEndpoint;
import org.shirdrn.streaming.server.common.MessageDispatcher;
import org.shirdrn.streaming.server.constants.ServerConstants;
import org.shirdrn.streaming.server.constants.ServerKeys;
import org.shirdrn.streaming.utils.ReflectionUtils;

import com.google.common.base.Preconditions;

/**
 * Streaming server side: 
 * it's responsible for receiving
 * push message events, and processing them.
 * 
 * @author yanjun
 */
public class StreamingServer extends StreamingEndpoint {

	private static final Log LOG = LogFactory.getLog(StreamingServer.class);
	private final Configuration serverConfig;
	private final MessageDispatcher<?> messageDispatcher;
	private final SocketAcceptor acceptor = new NioSocketAcceptor();
	
	@SuppressWarnings("unchecked")
	public StreamingServer() throws Exception {
		super();
		serverConfig = new PropertiesConfiguration(ServerConstants.SERVER_CONFIG);
		String dispatcherClassName = serverConfig.getString(
				ServerKeys.SERVER_MESSAGE_DISPATCHER_CLASS, 
				"org.shirdrn.streaming.server.DefaultMessageDispatcher");
		Preconditions.checkArgument(dispatcherClassName != null, "Dispatcher class MUST no be null!");
		LOG.info("Configured message dispatcher class: " + dispatcherClassName);
		messageDispatcher = (MessageDispatcher<FileLineMessage>) ReflectionUtils.newInstance(dispatcherClassName, 
				this.getClass().getClassLoader(), MessageDispatcher.class, serverConfig);
		LOG.info("Registered message dipatcher instance: " + messageDispatcher);
	}

	@Override
	public void start() throws Exception {
		LOG.info("Configure streaming server...");
		acceptor.setReuseAddress(true);
		acceptor.getFilterChain()
			.addLast("codec", new ProtocolCodecFilter(new ObjectSerializationCodecFactory()));
		acceptor.setHandler(new ServerReceiveHandler(messageDispatcher));
		
		InetSocketAddress addr = super.getSocketAddress();
		LOG.info("Server binds to: " + addr);
        acceptor.bind(addr);
        LOG.info("Streaming server started!");
        
        LOG.info("Starting message dispatcher...");
        messageDispatcher.start();
        LOG.info("Started!");
	}

	@Override
	public void stop() throws Exception {
		messageDispatcher.stop();
		acceptor.unbind();
	}
	
	public static void main(String[] args) throws Exception {
		try {
			final StreamingServer server = new StreamingServer();
			server.start();
		} catch (Exception e) {
			LOG.info("Fail to start streming server: ", e);
		}
	}
	
}
