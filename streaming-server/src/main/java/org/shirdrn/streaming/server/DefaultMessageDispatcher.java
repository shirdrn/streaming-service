package org.shirdrn.streaming.server;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.shirdrn.streaming.common.FileLineMessage;
import org.shirdrn.streaming.server.common.AbstractMessageDispatcher;

public class DefaultMessageDispatcher extends
		AbstractMessageDispatcher<FileLineMessage> {

	private static final Log LOG = LogFactory.getLog(DefaultMessageDispatcher.class);
	
	public DefaultMessageDispatcher(Configuration serverConfig) {
		super(serverConfig);
	}

	@Override
	public void dispatch(FileLineMessage message) {
		LOG.info("Dispatch: message=" + message);
	}

}
