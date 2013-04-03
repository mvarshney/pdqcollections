package pdqninja.pdqio;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileWriter;
import java.io.RandomAccessFile;
import java.util.UUID;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class SplitterTest {
	File file;
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
		file = new File(UUID.randomUUID().toString());
		file.deleteOnExit();
		file.createNewFile();
		
		FileWriter writer = new FileWriter(file);
		writer.write("abcd 1234 abcd\n");
		writer.write("abcd 1234\n");
		writer.write("abcd 1234\n");
		writer.close();
	}

	@After
	public void tearDown() throws Exception {
		file.delete();
	}

	@Test
	public void testDelimitedStartEnd() throws Exception {
		Splitter sp = new DelimitedSplitter("bc", "34");
		RandomAccessFile raf = new RandomAccessFile(file, "r");
		
		assertSplit(sp, raf, 1000, 1, 35);
		raf.seek(0);
		
		long blocksize = 4;
		
		assertSplit(sp, raf, blocksize, 1, 9);
		assertSplit(sp, raf, blocksize, 12, 24);
		assertSplit(sp, raf, blocksize, 27, 34);
		assertSplit(sp, raf, blocksize, -1, -1);
		
		blocksize = 15;
		raf.seek(0);
		assertSplit(sp, raf, blocksize, 1, 24);
		assertSplit(sp, raf, blocksize, 27, 35);
		assertSplit(sp, raf, blocksize, -1, -1);
		
		sp = new DelimitedSplitter("ab", "cd");
		blocksize = 1;
		raf.seek(0);
		assertSplit(sp, raf, blocksize, 0, 4);
		assertSplit(sp, raf, blocksize, 11, 14);
		assertSplit(sp, raf, blocksize, 16, 19);
		assertSplit(sp, raf, blocksize, 26, 29);
		assertSplit(sp, raf, blocksize, -1, -1);
		
		sp = new DelimitedSplitter("cd", "ab");
		raf.seek(0);
		blocksize = 4;
		assertSplit(sp, raf, blocksize, 2, 12);
		assertSplit(sp, raf, blocksize, 13, 27);
		assertSplit(sp, raf, blocksize, 28, 35);
		assertSplit(sp, raf, blocksize, -1, -1);
		
		raf.close();
	}
	

	@Test
	public void testDelimitedEnd() throws Exception {
		Splitter sp = DelimitedSplitter.NewLineDelimitedSplitter;
		RandomAccessFile raf = new RandomAccessFile(file, "r");
		
		assertSplit(sp, raf, 1000, 0, 35);
		raf.seek(0);
		
		long blocksize = 4;
		
		assertSplit(sp, raf, blocksize, 0, 15);
		assertSplit(sp, raf, blocksize, 15, 25);
		assertSplit(sp, raf, blocksize, 25, 35);
		assertSplit(sp, raf, blocksize, -1, -1);
		
		blocksize = 20;
		raf.seek(0);
		assertSplit(sp, raf, blocksize, 0, 25);
		assertSplit(sp, raf, blocksize, 25, 35);
		assertSplit(sp, raf, blocksize, -1, -1);
		
		raf.close();
	}
	
	@Test
	public void testFixedSize() throws Exception {
		Splitter sp = new FixedSizeSplitter();
		long blocksize = 10;
		RandomAccessFile raf = new RandomAccessFile(file, "r");
		
		assertSplit(sp, raf, blocksize, 0, 10);
		assertSplit(sp, raf, blocksize, 10, 20);
		assertSplit(sp, raf, blocksize, 20, 30);
		assertSplit(sp, raf, blocksize, 30, 35);
		assertSplit(sp, raf, blocksize, -1, -1);
		
		blocksize = 12;
		raf.seek(0);
		assertSplit(sp, raf, blocksize, 0, 12);
		assertSplit(sp, raf, blocksize, 12, 24);
		assertSplit(sp, raf, blocksize, 24, 35);
		assertSplit(sp, raf, blocksize, -1, -1);
		
		sp = new FixedSizeSplitter(7);
		raf.seek(0);
		assertSplit(sp, raf, blocksize, 7, 19);
		assertSplit(sp, raf, blocksize, 19, 31);
		assertSplit(sp, raf, blocksize, 31, 35);
		assertSplit(sp, raf, blocksize, -1, -1);
		
		sp = new FixedSizeSplitter(7, 3);
		raf.seek(0);
		assertSplit(sp, raf, blocksize, 7, 21);
		assertSplit(sp, raf, blocksize, 21, 33);
		assertSplit(sp, raf, blocksize, 33, 35);
		assertSplit(sp, raf, blocksize, -1, -1);
		
		raf.close();
	}
	
	void assertSplit(Splitter sp, RandomAccessFile raf,
			long blocksize,
			long begin, long end) throws Exception {
		long pos;
		
		pos = sp.splitBegin(raf);
		assertEquals(begin, pos);
		if (begin == -1) return;
		raf.seek(pos);
		
		pos = sp.splitEnd(raf, blocksize);
		assertEquals(end, pos);
		raf.seek(pos);
		
	}

}
