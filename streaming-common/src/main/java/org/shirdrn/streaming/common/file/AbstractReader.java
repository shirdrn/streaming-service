package org.shirdrn.streaming.common.file;

import java.io.File;
import java.io.IOException;

public abstract class AbstractReader<T> extends AbstractFileAccessor implements FileReadable<T> {

	public AbstractReader(File file) {
		super(file);
	}
	
	@Override
	public void open() throws IOException {
		super.open("r");		
	}

}
