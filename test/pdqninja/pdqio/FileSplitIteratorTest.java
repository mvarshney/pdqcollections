package pdqninja.pdqio;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class FileSplitIteratorTest {
	Map<String, File> Dirs = new HashMap<String, File>();
	Map<String, File> Files = new HashMap<String, File>();
	
	public static class Split {
		RandomAccessFile raf = null;
		long length = -1;
		public Split(RandomAccessFile raf, long length) {
			this.raf = raf;
			this.length = length;
		}
		
		public void assertSplit(long begin, long length) throws IOException {
			assertEquals(begin, raf.getFilePointer());
			assertEquals(length, this.length);
		}
		
	}
	
	public static class SplitIterator extends FileSplitIterator<Split> {
		SplitIterator(FileStoreIterator fsIterator, long blocksize,
				Splitter splitter) throws IOException {
			super(fsIterator, blocksize, splitter);
		}

		@Override
		Split createSplit(RandomAccessFile raf, long length) throws IOException {
			return new Split(raf, length);
		}
	}
	
	@Before
	public void setUp() throws Exception {
		String[] dirnames = {"a", "b", "c"};
		
		for (String dirname: dirnames) {
			File dir;
			if (dirname.equals("c")) {
				dir = new File(
						Dirs.get("a"),
						UUID.randomUUID().toString());
			} else {
				dir = new File(UUID.randomUUID().toString());
			}
			dir.mkdir();
			dir.deleteOnExit();
			Dirs.put(dirname, dir);	
		}
		
		String[] inDirs = {"a", "a", "b", "b", "c", "c"};
		
		for (int i = 1; i <= inDirs.length; i++) {
			File dir = Dirs.get(inDirs[i - 1]);
			File file = new File(dir, Integer.toString(i));
			file.deleteOnExit();
			file.createNewFile();
			Files.put(inDirs[i - 1] + "/" + i, file);
			write(file, i);	
		}
	}
	
	void write(File file, int num) throws IOException {
		FileWriter writer = new FileWriter(file);
		for (int i = 0; i < 4; i++) {
			for (int j = 1; j < 10; j++) {
				writer.write(Integer.toString(num));
			}
			writer.write("\n");
		}
		writer.close();
	}
	
	@After
	public void tearDown() throws Exception {
		for (File file: Files.values()) {
			file.delete();
		}
		for (File file: Dirs.values()) {
			file.delete();
		}
	}
	
	@Test
	public void test1File1Dir() throws IOException {
		File[] dirs = {Dirs.get("a")};
		int[] weights = {1};
		File[] files = {Files.get("a/1")};
		long blocksize = 5;
		FileStoreIterator fsIterator = 
				new FileStoreIterator(dirs, weights, Arrays.asList(files));
		
		SplitIterator it = new SplitIterator(fsIterator, blocksize,
				DelimitedSplitter.NewLineDelimitedSplitter);
		
		Split s;
		
		for (int i = 0; i <= 30; i += 10) {
			assertTrue(it.hasNext());
			s = it.next();
			s.assertSplit(i, 10);	
		}
			
		assertFalse(it.hasNext());
	}
	
	@Test
	public void testAbsentFiles1Dir() throws IOException {
		File dir = Dirs.get("a");
		File[] dirs = {dir};
		int[] weights = {1};
		File[] files = {new File(dir, "absent1"),
				Files.get("a/1"),
				new File(dir, "absent2")};
		long blocksize = 5;
		FileStoreIterator fsIterator = 
				new FileStoreIterator(dirs, weights, Arrays.asList(files));
		
		SplitIterator it = new SplitIterator(fsIterator, blocksize,
				DelimitedSplitter.NewLineDelimitedSplitter);
		
		Split s;
		
		for (int i = 0; i <= 30; i += 10) {
			assertTrue(it.hasNext());
			s = it.next();
			s.assertSplit(i, 10);	
		}
			
		assertFalse(it.hasNext());
	}
	
	@Test
	public void testNoFiles() throws IOException {
		File dir = Dirs.get("a");
		File[] dirs = {dir};
		int[] weights = {1};
		File[] files = {};
		long blocksize = 5;
		
		// No files to begin with
		FileStoreIterator fsIterator = 
				new FileStoreIterator(dirs, weights, Arrays.asList(files));
		
		SplitIterator it = new SplitIterator(fsIterator, blocksize,
				DelimitedSplitter.NewLineDelimitedSplitter);
		
		assertFalse(it.hasNext());
		
		// One file but it does not exists
		File[] files2 = {new File(dir, "absent")};
		fsIterator = 
				new FileStoreIterator(dirs, weights, Arrays.asList(files2));
		
		it = new SplitIterator(fsIterator, blocksize,
				DelimitedSplitter.NewLineDelimitedSplitter);
		
		assertFalse(it.hasNext());
		
		// One file but it is not within the specified directory
		File[] files3 = {new File("/tmp")};
		fsIterator = 
				new FileStoreIterator(dirs, weights, Arrays.asList(files3));
		
		it = new SplitIterator(fsIterator, blocksize,
				DelimitedSplitter.NewLineDelimitedSplitter);
		
		assertFalse(it.hasNext());
	}
	
	@Test
	public void test2Files1Dir() throws IOException {
		File[] dirs = {Dirs.get("a"), Dirs.get("b")};
		File[] files = {Files.get("a/1"), Files.get("a/2")};
		int[] weights = {1, 1};
		
		long blocksize = 5;
		
		// No files to begin with
		FileStoreIterator fsIterator = 
				new FileStoreIterator(dirs, weights, Arrays.asList(files));
		
		SplitIterator it = new SplitIterator(fsIterator, blocksize,
				DelimitedSplitter.NewLineDelimitedSplitter);
		
		for (int i = 0; i < 4; i++) {
			assertLine(it, "111111111");
		}
		
		for (int i = 0; i < 4; i++) {
			assertLine(it, "222222222");
		}
		
		assertFalse(it.hasNext());
	}

	@Test
	public void testParallelRead() throws IOException {
		File[] dirs = {Dirs.get("a"), Dirs.get("b")};
		File[] files = {Files.get("a/1"), Files.get("b/3")};
		int[] weights = {1, 1};
		
		long blocksize = 5;
		
		// No files to begin with
		FileStoreIterator fsIterator = 
				new FileStoreIterator(dirs, weights, Arrays.asList(files));
		
		SplitIterator it = new SplitIterator(fsIterator, blocksize,
				DelimitedSplitter.NewLineDelimitedSplitter);
			
		for (int i = 0; i < 4; i++) {
			assertLine(it, "111111111");
			assertLine(it, "333333333");
		}
		
		assertFalse(it.hasNext());
	}
	
	@Test
	public void test3Files2Dir() throws IOException {
		File[] dirs = {Dirs.get("a"), Dirs.get("b"), Dirs.get("c")};
		File[] files = {Files.get("a/1"), Files.get("a/2"), Files.get("c/5")};
		int[] weights = {1, 1, 1};
		
		long blocksize = 5;
		
		// No files to begin with
		FileStoreIterator fsIterator = 
				new FileStoreIterator(dirs, weights, Arrays.asList(files));
		
		SplitIterator it = new SplitIterator(fsIterator, blocksize,
				DelimitedSplitter.NewLineDelimitedSplitter);
		
		for (int i = 0; i < 4; i++) {
			assertLine(it, "111111111");
			assertLine(it, "555555555");
		}
		
		for (int i = 0; i < 4; i++) {
			assertLine(it, "222222222");
		}
		
		assertFalse(it.hasNext());
	}
	
	@Test
	public void test3Files2DirWeighted() throws IOException {
		File[] dirs = {Dirs.get("a"), Dirs.get("b"), Dirs.get("c")};
		File[] files = {Files.get("a/1"), Files.get("a/2"), Files.get("c/5")};
		int[] weights = {2, 2, 1};
		
		long blocksize = 5;
		
		// No files to begin with
		FileStoreIterator fsIterator = 
				new FileStoreIterator(dirs, weights, Arrays.asList(files));
		
		SplitIterator it = new SplitIterator(fsIterator, blocksize,
				DelimitedSplitter.NewLineDelimitedSplitter);
		
			assertLine(it, "111111111");
			assertLine(it, "111111111");
			assertLine(it, "555555555");
			assertLine(it, "111111111");
			assertLine(it, "111111111");
			assertLine(it, "555555555");
			assertLine(it, "222222222");
			assertLine(it, "222222222");
			assertLine(it, "555555555");
			assertLine(it, "222222222");
			assertLine(it, "222222222");
			assertLine(it, "555555555");
		
			assertFalse(it.hasNext());
	}
	
	void assertLine(SplitIterator it, String expectedLine) throws IOException {
		assertTrue(it.hasNext());
		Split split = it.next();
		String line = split.raf.readLine();
		assertEquals(expectedLine, line);
	}
}
