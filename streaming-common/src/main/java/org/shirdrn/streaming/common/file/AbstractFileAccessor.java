package org.shirdrn.streaming.common.file;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

public abstract class AbstractFileAccessor {

	protected final File file;
	protected RandomAccessFile randomAccessFile;
	
	public AbstractFileAccessor(File file) {
		this.file = file;
	}
	
	public void open(String mode) throws IOException {
		randomAccessFile = new RandomAccessFile(this.file, mode);
	}
	
	public long getLength() throws IOException {
		return randomAccessFile.length();
	}
	
	public long getPosition() throws IOException {
		return randomAccessFile.getFilePointer();
	}
	
	public void close() throws IOException {
		randomAccessFile.close();
	}

}
