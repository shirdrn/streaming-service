package org.shirdrn.streaming.agent.constants;

public interface AgentKeys {

	// Used for Agent Side
	
	String  AGENT_CLIENT_PUSH_CLIENT_CLASS= "agent.client.push.client.class";
	String AGENT_CLIENT_CONNECT_RETRY_TIMES = "agent.client.connect.retry.times";
	String AGENT_CLIENT_CONNECT_RETRY_SLEEP = "agent.client.connect.retry.sleep";
	String AGENT_CLIENT_QUEUE_TAKE_INTERVAL = "agent.client.queue.take.interval";
	String AGENT_METADATA_ROOT_DIR = "agent.metadata.root.dir";
	String AGENT_MESSAGE_QUEUE_CAPACITY = "agent.message.queue.capacity";
	String AGENT_CLIENT_SEND_SLEEP_INTERVAL = "agent.client.send.sleep.interval";
	String AGENT_MESSAGE_QUEUE_OFFER_OR_POLL_WAIT_INTERVAL = "agent.message.queue.offer.or.poll.wait.interval";
	String AGENT_READER_WORKER_WAIT_FILE_BEING_WRITTEN_INTERVAL = "agent.reader.work.wait.file.being.written.interval";
	String AGENT_READER_WORKER_WAIT_READ_TASK_ARRIVAL_INTERVAL = "agent.reader.worker.wait.read.task.arrival.interval";
	String AGENT_CALLBACK_TX_QUEUE_CHECK_INTERVAL = "agent.callback.tx.queue.check.interval";
	
}
