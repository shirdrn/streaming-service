package org.shirdrn.streaming.file;

import java.io.Closeable;
import java.io.IOException;

public interface FileReadable<T> extends Closeable {

	void open() throws IOException;
	boolean hasNext() throws IOException;
	T next() throws IOException;
}
