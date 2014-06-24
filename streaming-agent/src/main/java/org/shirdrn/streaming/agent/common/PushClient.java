package org.shirdrn.streaming.agent.common;

import org.shirdrn.streaming.common.LifecycleAware;

public interface PushClient extends LifecycleAware{

	int getId();
	void setIsConnected(boolean isConnected);
	boolean isConnected();
	void reconnect(Exception cause);
}
