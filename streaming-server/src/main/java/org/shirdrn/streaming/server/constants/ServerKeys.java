package org.shirdrn.streaming.server.constants;

public interface ServerKeys {

	// Used for Server Side
	String SERVER_MESSAGE_DISPATCHER_CLASS = "server.message.dispatcher.class";
	String SERVER_MESSAGE_QUEUE_CAPACITY = "server.message.queue.capacity";
	
	String SERVER_MEMCACHED_SERVERS = "server.memcached.servers";
	String SERVER_MEMCACHED_WEIGHTS = "server.memcached.weights";
	String SERVER_MEMCACHED_SOCKET_CONNECT_TIMEOUT = "server.memcached.socket.connect.timeout";
	String SERVER_MEMCACHED_SOCKET_READ_TIMEOUT = "server.memcached.socket.read.timeout";
	String SERVER_MEMCACHED_CONNECTION_INIT_COUNT = "server.memcached.connection.init.count";
	String SERVER_MEMCACHED_CONNECTION_MIN_COUNT = "server.memcached.connection.min.count";
	String SERVER_MEMCACHED_CONNECTION_MAX_COUNT = "server.memcached.connection.max.count";
	String SERVER_MEMCACHED_CONNECTION_MAX_IDLE = "server.memcached.connection.max.idle";
	String SERVER_CACHE_UPDATE_INTERVAL = "server.cache.update.interval";
	
	
}
