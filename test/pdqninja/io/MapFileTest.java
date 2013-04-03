package pdqninja.io;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import org.junit.Test;

public class MapFileTest {
	
	@Test
	public void testReadWrite() throws IOException, ClassNotFoundException {
		File file = new File(UUID.randomUUID().toString());
		file.createNewFile();
		
		MapFile<Integer> writer = 
				new MapFile<Integer>(file, "rw");
		
		for (int i = 0; i < 10; i++) {
			writer.write(i);
		}
		
		writer.close();
		
		MapFile<Integer> reader =
				new MapFile<Integer>(file, "r");
		
		
		for (int i = 0; i < 10; i++) {
			assertEquals(i, reader.read());
		}
		
		reader.close();
		file.delete();
	}
	
	@Test
	public void testSeek() throws IOException, ClassNotFoundException {
		File file = new File(UUID.randomUUID().toString());
		file.createNewFile();
		
		MapFile<Integer> writer = 
				new MapFile<Integer>(file, "rw");
		
		for (int i = 0; i < 10; i++) {
			writer.write(i);
		}
		
		writer.close();
		
		MapFile<Integer> reader =
				new MapFile<Integer>(file, "r");
		
		
		for (int i = 0; i < 10; i++) {
			assertEquals(i, reader.read());
		}
		
		reader.seek(0);
		
		for (int i = 0; i < 10; i++) {
			assertEquals(i, reader.read());
		}
		
		reader.close();
		file.delete();
	}
	
	@Test
	public void testIndex() throws IOException, ClassNotFoundException {
		File file = new File(UUID.randomUUID().toString());
		file.createNewFile();
		
		MapFile<Integer> writer = 
				new MapFile<Integer>(file, "rw",
						new MapFileIndex<Integer>(), 4, 100, 0);
		
		for (int i = 0; i < 10; i++) {
			writer.write(i);
		}
		
		writer.close();
		
		MapFileIndex<Integer> index = writer.getIndex();
		
		MapFile<Integer> reader =
				new MapFile<Integer>(file, "r");
		
		assertEquals(10, index.size());
		
		
		for (int i = 9; i > 0; i--) {
			long off = index.getIndex(i).getOffset();
			reader.seek(off);
			assertEquals(i, reader.read());
		}
		
		
		reader.close();
		file.delete();
	}
	
	@Test
	public void testAppend() throws IOException, ClassNotFoundException {
		File file = new File(UUID.randomUUID().toString());
		file.createNewFile();
		
		// Open file for the first time
		MapFile<Integer> writer = 
				new MapFile<Integer>(file, "rw");
		
		for (int i = 0; i < 5; i++) {
			writer.write(i);
		}
		
		writer.close();
		
		// Reopen and write more
		writer = new MapFile<Integer>(file, "rw");
		writer.seek(writer.length());
		
		for (int i = 5; i < 10; i++) {
			writer.write(i);
		}
		
		writer.close();
		
		// Verify that everything is written
		MapFile<Integer> reader =
				new MapFile<Integer>(file, "r");
		for (int i = 0; i < 10; i++) {
			assertEquals(i, reader.read());
		}
		
		reader.close();
		file.delete();
	}

}
