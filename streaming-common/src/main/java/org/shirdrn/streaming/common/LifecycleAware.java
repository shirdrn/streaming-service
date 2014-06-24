package org.shirdrn.streaming.common;

/**
 * Service or component lifecycle management. 
 * 
 * @author yanjun
 */
public interface LifecycleAware {

	/**
	 * Starts a service or component.
	 */
	public void start() throws Exception;

	/**
	 * Stops a service or component.
	 */
	public void stop() throws Exception;

}
