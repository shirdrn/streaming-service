package org.shirdrn.streaming.common;


/**
 * Basic representation of a data object. Provides access to data as it flows
 * through the system.
 */
public interface Message<T> {

	void setType(int type);
	int getType();
	
}
