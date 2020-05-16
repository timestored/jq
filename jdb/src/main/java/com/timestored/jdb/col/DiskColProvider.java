package com.timestored.jdb.col;

import java.io.File;
import java.io.IOException;
import java.nio.MappedByteBuffer;

import com.google.common.base.Preconditions;
import com.timestored.jdb.database.CType;

import lombok.ToString;

@ToString 
class DiskColProvider extends ColProvider {
		
		private static final long HEADER_SZ = 2;
		private File folder;

		public DiskColProvider(File file) {
			this.folder = Preconditions.checkNotNull(file);
			Preconditions.checkArgument(file.isDirectory());
		}
		
		private static void createEmptyCol(File file, short type) throws IOException {
			if(file.exists()) {
				file.delete();
			}
			file.createNewFile();
			Buff buff = new Buff(file);
			MappedByteBuffer mbb = buff.map(6, 0, RMode.WRITE);
			mbb.putShort(type);
			mbb.putInt(0);
			buff.close();
		}

		private File createFile(String identifier) throws IOException {
			File file = new File(folder, identifier);
			createEmptyCol(file, CType.INTEGER.getTypeNum());
			Preconditions.checkArgument(file.exists());
			return file;
		}

		@Override public ObjectCol createObjectCol(String identifier) throws IOException {
			throw new UnsupportedOperationException("Nested not supported on-disk yet");
			//TODO support on-disk nested
			//return new DiskObjectCol(createFile(identifier), HEADER_SZ);
		}

		@Override public CharacterCol createCharacterCol(String identifier) throws IOException {
			return new DiskCharacterCol(createFile(identifier), HEADER_SZ);
		}

		@Override public IntegerCol createIntegerCol(String identifier) throws IOException {
			return new DiskIntegerCol(createFile(identifier), HEADER_SZ);
		}
		
		@Override public DoubleCol createDoubleCol(String identifier) throws IOException {
			return new DiskDoubleCol(createFile(identifier), HEADER_SZ);
		}

		@Override public FloatCol createFloatCol(String identifier) throws IOException {
			return new DiskFloatCol(createFile(identifier), HEADER_SZ);
		}

		@Override public LongCol createLongCol(String identifier) throws IOException {
			return new DiskLongCol(createFile(identifier), HEADER_SZ);
		}


		@Override public ByteCol createByteCol(String identifier) throws IOException {
			return new DiskByteCol(createFile(identifier), HEADER_SZ);
		}

		@Override public ShortCol createShortCol(String identifier) throws IOException { 
			return new DiskShortCol(createFile(identifier), HEADER_SZ);
		} 
		

		@Override public StringCol createStringCol(String identifier) throws IOException {
			File f = createFile(identifier);
			IntegerCol ic = new DiskIntegerCol(f, HEADER_SZ);
			return new IntegerBackedStringCol(ic);
		}
	}