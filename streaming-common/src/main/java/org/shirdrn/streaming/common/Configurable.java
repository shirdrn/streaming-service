package org.shirdrn.streaming.common;

import org.apache.commons.configuration.Configuration;

public interface Configurable {

	void setConfig(Configuration config);
	Configuration getConfig();
}
