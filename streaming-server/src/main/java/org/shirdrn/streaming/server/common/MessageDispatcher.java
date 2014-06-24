package org.shirdrn.streaming.server.common;

import org.shirdrn.streaming.common.LifecycleAware;

/**
 * Used for loose-coupling the MINA handler and subsequent processing, such as 
 * being handled by business handler, demand on storage layer.
 * 
 * @author yanjun
 *
 * @param <M>
 */
public interface MessageDispatcher<M> extends LifecycleAware{

	void dispatch(M message);
	
}
