package org.shirdrn.streaming.file;

import java.io.File;
import java.io.IOException;

public abstract class AbstractWriter<T> extends AbstractFileAccessor implements FileWriteable<T> {

	public AbstractWriter(File file) {
		super(file);
	}
	
	@Override
	public void open() throws IOException {
		super.open("rw");		
	}

	@Override
	public void flush() throws IOException {
		randomAccessFile.getChannel().force(false);		
	}
	
}
