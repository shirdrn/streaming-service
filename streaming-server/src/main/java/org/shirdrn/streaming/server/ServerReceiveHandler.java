package org.shirdrn.streaming.server;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.mina.core.service.IoHandlerAdapter;
import org.apache.mina.core.session.IoSession;
import org.shirdrn.streaming.common.FileLineMessage;
import org.shirdrn.streaming.server.common.MessageDispatcher;

public class ServerReceiveHandler extends IoHandlerAdapter {

	private static final Log LOG = LogFactory.getLog(ServerReceiveHandler.class);
	private final MessageDispatcher<FileLineMessage> messageDispatcher;
	
	@SuppressWarnings("unchecked")
	public ServerReceiveHandler(MessageDispatcher<?> messageDispatcher) {
		this.messageDispatcher = (MessageDispatcher<FileLineMessage>) messageDispatcher;
	}
	
	@Override
	public void sessionOpened(IoSession session) throws Exception {
		LOG.info("Server side session opened: session=" + session);
	}
	
	@Override
	public void messageReceived(IoSession session, Object message)
			throws Exception {
		if(message instanceof FileLineMessage) {
			FileLineMessage lineMessage = (FileLineMessage) message;
			LOG.info("Recv: message=[" + lineMessage + "]");
			if(!lineMessage.getLine().trim().isEmpty()) {
				messageDispatcher.dispatch(lineMessage);
			} else {
				LOG.warn("Line content is empty, discard it.");
			}
			session.write(lineMessage.getFileMeta());
			LOG.debug("Ack done: fileMeta=[" + lineMessage.getFileMeta() + "]");
			LOG.info("Ack done: txid=" + lineMessage.getFileMeta().getTxid());
		} else {
			LOG.warn("Server unknown message: " + message);
		}
	}

	@Override
	public void messageSent(IoSession session, Object message) throws Exception {
		LOG.debug("Server ack sent out: " + message);
	}

	@Override
	public void sessionClosed(IoSession session) throws Exception {
		// TODO Auto-generated method stub
		super.sessionClosed(session);
	}

	@Override
	public void exceptionCaught(IoSession session, Throwable cause)
			throws Exception {
		if(LOG.isDebugEnabled()) {
			LOG.error("", cause);
		}
		LOG.error("EXCEPTION:" + cause.getMessage());
		LOG.info("Closed: session=" + session);
	}

}
