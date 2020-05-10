package com.timestored.jdb.server;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import javax.annotation.Nullable;

import com.google.common.io.LittleEndianDataInputStream;
import com.google.common.io.LittleEndianDataOutputStream;
import com.timestored.jdb.database.JUtils;
import com.timestored.jdb.col.StringMap;
import com.timestored.jdb.database.IpcDataReader;
import com.timestored.jdb.database.IpcDataWriter;

import io.netty.buffer.ByteBuf;
import lombok.Data;

/**
 * The IPC format for passing messages between client <-> Server. Format is:
 */
@Data class MsgWrapper {
	
	public static final int MINIMUM_BYTES_NEEDED = 8;
	
	private final ByteOrder byteOrder;
	private final MsgType msgType;
	private final boolean compressed;
	private final Object data;
	

	public void writeTo(ByteBuf out) {
        try {
        	// TODO could get more efficient by calculating the size and reusing buffers
        	ByteArrayOutputStream bo = new ByteArrayOutputStream(1024);
        	DataOutput dout = null;
			boolean isBigEndian = byteOrder.equals(ByteOrder.BIG_ENDIAN);
        	if(byteOrder.equals(ByteOrder.BIG_ENDIAN)) {
        		dout = new DataOutputStream(bo);
        	} else {
        		dout = new LittleEndianDataOutputStream(bo);
        	}
        	
        	IpcDataWriter saver = new IpcDataWriter(dout);
        	saver.visit(data);
			
			out.writeByte(isBigEndian ? 0 : 1);  // 0=big endian 1=little endian PC
			out.writeByte(2); // 0=async 1=sync 2=response
			out.writeByte(0); // 0=normal 1=compressed
			out.writeByte(0); // unused
			int dataLength = 8 + bo.size();
	        if(isBigEndian) {
	        	out.writeInt(dataLength);
	        } else {
	        	out.writeIntLE(dataLength);
	        }
	        byte[] buff = bo.toByteArray();
//	        System.out.println(JUtils.toString(buff));
	        out.writeBytes(buff);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	/**
	 * If enough data is in the buffer to retrieve the full object do so, else return null.
	 */
	@Nullable public static MsgWrapper readFrom(ByteBuf in) {
	   
		if (in.readableBytes() < MsgWrapper.MINIMUM_BYTES_NEEDED) {
            return null;
        }
		
        boolean bigEndian = 0 == in.readUnsignedByte(); // 0=big endian 1=little endian PC
        ByteOrder byteOrder = bigEndian ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN;
        MsgType msgType = MsgType.fromFlag(in.readUnsignedByte());
        boolean compressed = in.readUnsignedByte() == 1;
        in.readUnsignedByte(); // unused
        int dataLength = bigEndian ? in.readInt() : in.readIntLE();
        int remainingDataSize = dataLength - 8; // they send a value that includes the header itself
        int readableBytes = in.readableBytes();
        
        if (readableBytes >= remainingDataSize) {
        	byte[] data = new byte[(int)remainingDataSize];
            in.readBytes(data);
            
            if(compressed) {
            	data = uncompress(data, byteOrder);
            }
            
            
            InputStream bi = new ByteArrayInputStream(data);
            DataInput din = bigEndian ? new DataInputStream(bi) : new LittleEndianDataInputStream(bi);
            Object o = null;
			try {
				o = IpcDataReader.read(din);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
            
        	return new MsgWrapper(byteOrder, msgType, compressed, o);
        }
        
		return null;
	}



	
	private static byte[] uncompress( final byte[] compressedData, final ByteOrder endianess )  {
        // size of the uncompressed message is encoded on first 4 bytes
        // size has to be decreased by header length (8 bytes)
        final ByteBuffer byteBuffer = ByteBuffer.wrap(compressedData, 0, 4);
        byteBuffer.order(endianess);
        final int uncompressedSize = -8 + byteBuffer.getInt();

        if ( uncompressedSize <= 0 ) {
            throw new RuntimeException("Uncompression Error.");
        }

        final byte[] uncompressed = new byte[uncompressedSize];
        final int[] buffer = new int[256];
        short i = 0;
        int n = 0, r = 0, f = 0, s = 0, p = 0, d = 4;

        while ( s < uncompressedSize ) {
            if ( i == 0 ) {
                f = 0xff & compressedData[d++];
                i = 1;
            }
            if ( (f & i) != 0 ) {
                r = buffer[0xff & compressedData[d++]];
                uncompressed[s++] = uncompressed[r++];
                uncompressed[s++] = uncompressed[r++];
                n = 0xff & compressedData[d++];
                for ( int m = 0; m < n; m++ ) {
                    uncompressed[s + m] = uncompressed[r + m];
                }
            } else {
                uncompressed[s++] = compressedData[d++];
            }
            while ( p < s - 1 ) {
                buffer[(0xff & uncompressed[p]) ^ (0xff & uncompressed[p + 1])] = p++;
            }
            if ( (f & i) != 0 ) {
                p = s += n;
            }
            i *= 2;
            if ( i == 256 ) {
                i = 0;
            }
        }

        return uncompressed;
    }

}