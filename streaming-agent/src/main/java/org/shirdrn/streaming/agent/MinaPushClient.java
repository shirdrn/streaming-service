package org.shirdrn.streaming.agent;

import java.net.InetSocketAddress;
import java.util.concurrent.BlockingDeque;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.mina.core.future.ConnectFuture;
import org.apache.mina.core.service.IoHandler;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.filter.codec.serialization.ObjectSerializationCodecFactory;
import org.apache.mina.transport.socket.SocketConnector;
import org.apache.mina.transport.socket.nio.NioSocketConnector;
import org.shirdrn.streaming.agent.common.AbstractPushClient;
import org.shirdrn.streaming.agent.constants.AgentKeys;
import org.shirdrn.streaming.common.FileLineMessage;
import org.shirdrn.streaming.common.StreamingEndpoint;
import org.shirdrn.streaming.common.constants.StreamingKeys;

import com.google.common.base.Preconditions;

/**
 * MINA push client, process actual network communications.
 * Currently we use MINA framework as the network layer of architecture.
 * 
 * @author yanjun
 */
public class MinaPushClient  extends AbstractPushClient {

	private static final Log LOG = LogFactory.getLog(MinaPushClient.class);
	private final BlockingDeque<FileLineMessage> sendDeque;
	private final StreamingEndpoint endpoint;
	
	private final SocketConnector connector;
	private IoSession ioSession;
	private volatile boolean connected = false;
	private final int sendSleepInterval;
	
	public MinaPushClient(StreamingEndpoint endpoint, int id, int type, 
			FileReaderManager fileReaderManager) throws Exception {
		super(endpoint, id, type, fileReaderManager);
		this.endpoint = endpoint;
		// share the deque
		sendDeque = fileReaderManager.getTypedMessageQueue(type);
		Preconditions.checkArgument(sendDeque != null, "Coundn't find sendQ for: type=" + type);
		
		connector = new NioSocketConnector();
		sendSleepInterval = agentConfig.getInt(AgentKeys.AGENT_CLIENT_SEND_SLEEP_INTERVAL, 20);
	}
	
	@Override
	public void start() throws Exception {
		LOG.info("Starting push client: #" + id);
		// Connect
		connector.getFilterChain().addLast(
				"codec", new ProtocolCodecFilter(new ObjectSerializationCodecFactory()));
		IoHandler pushHandler = new ClientPushHandler(this, fileReaderManager);
		connector.setHandler(pushHandler);
		connect();
		loopForSend();
	}
	
	private void connect() {
		try {
			InetSocketAddress addr = endpoint.getSocketAddress();
			LOG.info("Client connects to: " + addr);
			ConnectFuture future = connector.connect(addr);
			future.awaitUninterruptibly();
			
			ioSession = future.getSession();
			ioSession.setAttribute(StreamingKeys.PUSH_CELINT_ID, id);
			LOG.info("Connected.");
		} catch (Exception e) {
			LOG.error("Fail to connect: ", e);
			reconnect(e);
		}
	}
	
	@Override
	public boolean isConnected() {
		return connected;
	}
	
	@Override
	public void setIsConnected(boolean isConnected) {
		connected = isConnected;
	}
	
	private int retries = 0;
	
	@Override
	public void reconnect(Exception cause) {
		// retry to connect to server
	    while (true) {
	    	try {
				if(retryPolicy.shouldRetry(cause, retries++)) {
					LOG.debug("Retry to connect: retries=" + retries);
					LOG.info("Try to connect remote peer...");
					// connect
					connect();
					if(connected) {
						LOG.info("Connected again.");
						reset();
						break;
					} else {
						continue;
					}
				} else {
					reset();
					int waitTime = 5 * retrySleepTime;
					LOG.info("Retry " + maxRetries + " times, failed, wait " + waitTime + " ms...");
					Thread.sleep(waitTime);
				}
			} catch (Exception e) {
				LOG.error("Retry to connect error: ", e);
			}
	    }
	}

	private void reset() {
		// reset retry times
		retries = 0;
		// reset retry policy for next time use
		resetRetryPolicy();
	}

	private void loopForSend() {
		final int waitPollAvailableInterval = 3000;
		while(true) {
			try {
				// if connection disconnected
				final int waitToBeConnnected = 2000;
				while(!connected) {
					LOG.warn("Wait " + waitToBeConnnected + " ms...");
					Thread.sleep(waitToBeConnnected);
					LOG.warn("Connecting...");
					
					// reconnect
					Exception cause = new Exception("");
					reconnect(cause);
				}
				
				// send events
				final FileLineMessage message = sendDeque.pollFirst(); // couldn't block
				LOG.debug("Poll: message=" + message);
				if(message == null) {
					Thread.sleep(waitPollAvailableInterval);
					LOG.debug("Send deque is empty, wait " + waitPollAvailableInterval + " ms...");
				} else {
					ioSession.write(message);
					LOG.info("Write done: meta=[" + message.getFileMeta() + "]");
					
					// limit send speed
					LOG.debug("Limit send speed, sleep " + sendSleepInterval + " ms...");
					Thread.sleep(sendSleepInterval);
				}
			} catch (Exception e) {
				LOG.error("Push client error to send: ", e);
			}
		}
	}

	@Override
	public void stop() throws Exception {
		connector.dispose();
	}

	public int getLogType() {
		return type;
	}
	
	@Override
	public String toString() {
		return "[id=" + id + ", type=" + type + "]";
	}

}
