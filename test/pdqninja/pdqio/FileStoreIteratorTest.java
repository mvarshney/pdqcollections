package pdqninja.pdqio;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import org.junit.Test;

public class FileStoreIteratorTest {

	@Test
	public void testIteratorTwoDirs() {
		// Two files, equal weights
		File[] dirs = {new File("a"), new File("b")};
		int[] weights = {2, 1};
		
		FileStoreIterator it = new FileStoreIterator(dirs, weights);
		assertEquals(it.next().getName(), "a");
		assertEquals(it.next().getName(), "a");
		assertEquals(it.next().getName(), "b");
		assertEquals(it.next().getName(), "a");
	}

	@Test
	public void testIteratorOneDir() {
		// Two files, equal weights
		File[] dirs = {new File("a")};
		int[] weights = {1};
		
		FileStoreIterator it = new FileStoreIterator(dirs, weights);
		assertEquals(it.next().getName(), "a");
		assertEquals(it.next().getName(), "a");
	}
	
	@Test
	public void testIteratorNoDir() {
		// Two files, equal weights
		File[] dirs = {};
		int[] weights = {};
		
		FileStoreIterator it = new FileStoreIterator(dirs, weights);
		assertFalse(it.hasNext());
	}
	
	@Test
	public void testGetFile2Dir() throws IOException {
		File[] dirs = {new File("a"), new File("b")};
		int[] weights = {2, 1};
		File[] files = {new File("a/1"), new File("a/2"), new File("b/3")};
		
		FileStoreIterator it =
				new FileStoreIterator(dirs, weights, Arrays.asList(files));
		
		it.next();
		assertEquals(it.getFiles().iterator().next().getName(), "1");
		assertEquals(it.getFiles().iterator().next().getName(), "1");
		it.next();
		assertEquals(it.getFiles().iterator().next().getName(), "1");
		assertEquals(it.getFiles().iterator().next().getName(), "1");
		
		it.next();
		assertEquals(it.getFiles().iterator().next().getName(), "3");
		assertEquals(it.getFiles().iterator().next().getName(), "3");
		
		it.next();
		assertEquals(it.getFiles().iterator().next().getName(), "1");
		assertEquals(it.getFiles().iterator().next().getName(), "1");
	}
	
	@Test
	public void testGetFile2DirOverlap() throws IOException {
		File[] dirs = {new File("a"), new File("a/b")};
		int[] weights = {2, 1};
		File[] files = {new File("a/1"), new File("a/2"), new File("a/b/3")};
		
		FileStoreIterator it =
				new FileStoreIterator(dirs, weights, Arrays.asList(files));
		
		it.next();
		assertEquals(it.getFiles().iterator().next().getName(), "1");
		assertEquals(it.getFiles().iterator().next().getName(), "1");
		it.next();
		assertEquals(it.getFiles().iterator().next().getName(), "1");
		assertEquals(it.getFiles().iterator().next().getName(), "1");
		
		it.next();
		assertEquals(it.getFiles().iterator().next().getName(), "3");
		assertEquals(it.getFiles().iterator().next().getName(), "3");
		
		it.next();
		assertEquals(it.getFiles().iterator().next().getName(), "1");
		assertEquals(it.getFiles().iterator().next().getName(), "1");
	}
	
	@Test
	public void testGetFile2DirRemove() throws IOException {
		File[] dirs = {new File("a"), new File("a/b")};
		int[] weights = {2, 1};
		File[] files = {new File("a/1"), new File("a/2"), new File("a/b/3")};
		
		FileStoreIterator it =
				new FileStoreIterator(dirs, weights, Arrays.asList(files));
		
		it.next();
		assertEquals(it.getFiles().iterator().next().getName(), "1");
		it.getFiles().remove(it.getFiles().iterator().next());
		assertEquals(it.getFiles().iterator().next().getName(), "2");
		it.next();
		assertEquals(it.getFiles().iterator().next().getName(), "2");
		assertEquals(it.getFiles().iterator().next().getName(), "2");
		
		it.next();
		assertEquals(it.getFiles().iterator().next().getName(), "3");
		assertEquals(it.getFiles().iterator().next().getName(), "3");
		
		it.next();
		assertEquals(it.getFiles().iterator().next().getName(), "2");
		assertEquals(it.getFiles().iterator().next().getName(), "2");
		it.getFiles().remove(it.getFiles().iterator().next());
		assertEquals(it.getFiles().size(), 0);
		
		it.next();
		it.next();
		assertEquals(it.getFiles().iterator().next().getName(), "3");
		it.getFiles().remove(it.getFiles().iterator().next());
		assertEquals(it.getFiles().size(), 0);
	}
}
