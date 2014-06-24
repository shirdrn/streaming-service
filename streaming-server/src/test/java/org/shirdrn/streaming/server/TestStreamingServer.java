package org.shirdrn.streaming.server;

import org.shirdrn.streaming.server.StreamingServer;

public class TestStreamingServer {

	public static void main(String[] args) throws Exception {
		final StreamingServer server = new StreamingServer();
		server.start();
	}
}
