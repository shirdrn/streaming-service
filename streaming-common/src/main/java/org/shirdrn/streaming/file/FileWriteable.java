package org.shirdrn.streaming.file;

import java.io.Closeable;
import java.io.IOException;

public interface FileWriteable<T> extends Closeable {

	void open() throws IOException;
	void write(T data) throws IOException;
	void flush() throws IOException;
}
