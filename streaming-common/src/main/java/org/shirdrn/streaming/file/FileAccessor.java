package org.shirdrn.streaming.file;

import java.io.Closeable;
import java.io.IOException;

public interface FileAccessor<T> extends FileWriteable<T>, FileReadable<T>, Closeable {

	long getLength() throws IOException;
	long getPosition() throws IOException;
	void open(String mode) throws IOException;
	
}
