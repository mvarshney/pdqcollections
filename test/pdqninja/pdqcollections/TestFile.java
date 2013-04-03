package pdqninja.pdqcollections;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class TestFile {

	static void createIntegerFile(File file, int lines, int wordsPerLine) throws IOException {
		BufferedWriter writer = new BufferedWriter(
				new FileWriter(file));
		
		int counter = 0;
		for (int i = 0; i < lines; i++) {
			for  (int j = 0; j < wordsPerLine; j++) {
				writer.write(String.format("%05d", counter++));
			}
			writer.write("\n");
		}
		
		writer.close();
	}
	
}
