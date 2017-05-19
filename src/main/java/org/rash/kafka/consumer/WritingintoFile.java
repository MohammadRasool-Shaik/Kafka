package org.rash.kafka.consumer;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public class WritingintoFile {
	private static WritingintoFile instance = null;

	private WritingintoFile() {

	}

	public void writeToFile(List<String> lines, String fileName) {
		try {
			Path path = Paths.get("D:\\tmp\\"+fileName+".txt");
			Files.write(path, lines, Charset.forName("UTF-8"));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static WritingintoFile getInstance() {
		if (instance == null) {
			instance = new WritingintoFile();
		}
		return instance;
	}
}
