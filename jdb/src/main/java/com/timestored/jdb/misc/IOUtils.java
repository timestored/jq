package com.timestored.jdb.misc;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import static java.nio.file.FileVisitResult.*;

public class IOUtils {
	public static void deleteFileOrFolder(final Path path) throws IOException {
		  Files.walkFileTree(path, new SimpleFileVisitor<Path>(){
		    @Override public FileVisitResult visitFile(final Path file, final BasicFileAttributes attrs)
		      throws IOException {
		      Files.delete(file);
		      return CONTINUE;
		    }

		    @Override public FileVisitResult visitFileFailed(final Path file, final IOException e) {
		      return handleException(e);
		    }

		    private FileVisitResult handleException(final IOException e) {
		      e.printStackTrace(); // replace with more robust error handling
		      return CONTINUE;
		    }

		    @Override public FileVisitResult postVisitDirectory(final Path dir, final IOException e)
		      throws IOException {
		      if(e!=null)return handleException(e);
		      Files.delete(dir);
		      return CONTINUE;
		    }
		  });
		};
}
