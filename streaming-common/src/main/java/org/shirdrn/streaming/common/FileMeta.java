package org.shirdrn.streaming.common;

import java.io.Serializable;

public class FileMeta implements Serializable {

	private static final long serialVersionUID = -7054514077561525680L;
	private int type;
	private int fileId;
	private int offset;
	private long txid;
	
	public FileMeta() {
		super();
	}

	public int getType() {
		return type;
	}

	public void setType(int type) {
		this.type = type;
	}
	
	public int getFileId() {
		return fileId;
	}

	public void setFileId(int fileId) {
		this.fileId = fileId;
	}

	public int getOffset() {
		return offset;
	}

	public void setOffset(int offset) {
		this.offset = offset;
	}

	public long getTxid() {
		return txid;
	}

	public void setTxid(long txid) {
		this.txid = txid;
	}

	public static FileMeta from(int type, int fileId, int offset, long txid) {
		FileMeta event = new FileMeta();
		event.setType(type);
		event.setFileId(fileId);
		event.setOffset(offset);
		event.setTxid(txid);
		return event;
	}
	
	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append("type=" + type).append(", ")
		.append("fileId=" + fileId).append(", ")
		.append("offset=" + offset).append(", ")
		.append("txid=" + txid);
		return sb.toString();
	}

}
