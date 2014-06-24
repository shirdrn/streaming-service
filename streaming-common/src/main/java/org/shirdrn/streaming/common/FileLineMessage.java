package org.shirdrn.streaming.common;

import java.io.Serializable;

/**
 * Persistable wrapper for Event
 */
public class FileLineMessage implements Message<String>, Serializable {

	private static final long serialVersionUID = -5228013204496964461L;
	private int type;
	private String line;
	private FileMeta fileMeta;

	@Override
	public void setType(int type) {
		this.type = type;
	}

	@Override
	public int getType() {
		return type;
	}

	public String getLine() {
		return line;
	}

	public void setLine(String line) {
		this.line = line;
	}

	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append("type=" + type + ", fileId=" + fileMeta.getFileId() +
				", offset=" + fileMeta.getOffset() + ", txid=" + fileMeta.getTxid());
		return sb.toString();
	}

	public FileMeta getFileMeta() {
		return fileMeta;
	}

	public void setFileMeta(FileMeta fileMeta) {
		this.fileMeta = fileMeta;
	}

}
