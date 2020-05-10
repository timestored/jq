package com.timestored.jdb.database;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public interface Transferable {
//	long getBytesRequiredForTransfer() throws IOException;
	void writeTransferal(DataOutput out) throws IOException;
	void readTransferal(DataInput in) throws IOException, ClassNotFoundException;
	short getType();
}
