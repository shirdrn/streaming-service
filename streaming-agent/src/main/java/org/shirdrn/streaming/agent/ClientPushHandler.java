package org.shirdrn.streaming.agent;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.mina.core.service.IoHandlerAdapter;
import org.apache.mina.core.session.IoSession;
import org.shirdrn.streaming.agent.common.PushClient;
import org.shirdrn.streaming.common.FileMeta;
import org.shirdrn.streaming.common.constants.StreamingKeys;

public class ClientPushHandler extends IoHandlerAdapter {

	private static final Log LOG = LogFactory.getLog(ClientPushHandler.class);
	private final PushClient client;
	private final FileReaderManager fileReaderManager;
	
	public ClientPushHandler(final PushClient client, FileReaderManager fileReaderManager) {
		super();
		this.client = client;
		this.fileReaderManager = fileReaderManager;
	}

	@Override
	public void sessionCreated(IoSession session) throws Exception {
		LOG.info("Client session created: session=" + session);
	}

	@Override
	public void sessionOpened(IoSession session) throws Exception {
		LOG.info("Client session opened: session=" + session);
		client.setIsConnected(true);
	}

	@Override
	public void sessionClosed(IoSession session) throws Exception {
		super.sessionClosed(session);
	}

	@Override
	public void messageSent(IoSession session, Object message) throws Exception {
		super.messageSent(session, message);
	}

	@Override
	public void messageReceived(IoSession session, Object message)
			throws Exception {
		int clientID = (Integer) session.getAttribute(StreamingKeys.PUSH_CELINT_ID);
		FileMeta meta = (FileMeta) message;
		LOG.info("Recv ack: clientId=" + clientID + ", ack=[" + meta + "]");
		fileReaderManager.getFileMetadataManager().completeTx(meta);
	}

	@Override
	public void exceptionCaught(IoSession session, Throwable cause)
			throws Exception {
		client.setIsConnected(false);
		
		LOG.info("Catches exception, try to reconnect...");
		client.reconnect(new Exception(cause));
		
		if(!client.isConnected()) {
			Thread.sleep(3000);
		}
	}
	
	
}
