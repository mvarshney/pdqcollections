package pdqninja.io;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.UUID;

import org.junit.Test;

public class MapFileIndexTest {

	@Test
	public void testEvenEntries() {
		MapFileIndex<Integer> id = new MapFileIndex<Integer>();
		
		for (int i = 0; i < 30; i++) {
			if ((i % 5) == 0)
				id.createIndex(i, i);
			else
				id.skip();
		}
		
		assertEquals(6, id.size());
		assertEquals(15, id.getIndex(3).getOffset());
		
		assertEquals(15, id.getIndexForKey(15).getOffset());
		assertEquals(15, id.getIndexForKey(16).getOffset());
		assertEquals(0, id.getIndexForKey(1).getOffset());
		assertEquals(25, id.getIndexForKey(201).getOffset());
		assertNull(id.getIndexForKey(-1));
		
		
		assertEquals(3, id.getIndexForKey(15, false));
		assertEquals(4, id.getIndexForKey(16, false));
		assertEquals(1, id.getIndexForKey(1, false));
		assertEquals(0, id.getIndexForKey(-1, false));
		assertEquals(-1, id.getIndexForKey(201, false));
	}
	
	@Test
	public void testOddEntries() {
		MapFileIndex<Integer> id = new MapFileIndex<Integer>();
		
		for (int i = 0; i <= 30; i++) {
			if ((i % 5) == 0)
				id.createIndex(i, i);
			else
				id.skip();
		}
		
		assertEquals(7, id.size());
		assertEquals(15, id.getIndex(3).getOffset());
		
		assertEquals(15, id.getIndexForKey(15).getOffset());
		assertEquals(15, id.getIndexForKey(16).getOffset());
		assertEquals(0, id.getIndexForKey(1).getOffset());
		assertEquals(30, id.getIndexForKey(201).getOffset());
		assertNull(id.getIndexForKey(-1));
		
		assertEquals(3, id.getIndexForKey(15, false));
		assertEquals(4, id.getIndexForKey(16, false));
		assertEquals(1, id.getIndexForKey(1, false));
		assertEquals(0, id.getIndexForKey(-1, false));
		assertEquals(-1, id.getIndexForKey(201, false));
	}
	
	@Test
	public void testSubIndex() {
		MapFileIndex<Integer> id = new MapFileIndex<Integer>();
		// Test with zero entries
		assertEquals(0, id.subIndex(0, 0).size());
		try {
			id.subIndex(10, 0);
			assert(false);
		} catch (IllegalArgumentException e) {
			
		} catch (Exception e) {
			assert(false);
		}
		
		// Test with one entry
		id.createIndex(0, 0);
		assertEquals(1, id.subIndex(0, 0).size());
		assertEquals(1, id.subIndex(-1, 0).size());
		assertEquals(1, id.subIndex(0, 1).size());
		assertEquals(1, id.subIndex(-1, 1).size());
		assertEquals(0, id.subIndex(100, 110).size());
		assertEquals(0, id.subIndex(-1, 1).getIndex(0).getOffset());
		
		// Test with two entries
		id.createIndex(5, 5);
		assertEquals(1, id.subIndex(0, 0).size());
		assertEquals(1, id.subIndex(-1, 0).size());
		assertEquals(2, id.subIndex(0, 1).size());
		assertEquals(2, id.subIndex(-1, 1).size());
		assertEquals(0, id.subIndex(-1, 1).getIndex(0).getOffset());
		
		assertEquals(1, id.subIndex(5, 5).size());
		assertEquals(2, id.subIndex(4, 5).size());
		assertEquals(2, id.subIndex(0, 5).size());
		assertEquals(2, id.subIndex(-1, 5).size());
		assertEquals(2, id.subIndex(0, 6).size());
		assertEquals(2, id.subIndex(-1, 6).size());
		assertEquals(0, id.subIndex(-1, 6).getIndex(0).getOffset());
		assertEquals(5, id.subIndex(-1, 6).getIndex(1).getOffset());
		
		// Test more random cases
		for (int i = 10; i <= 30; i+=5)
			id.createIndex(i, i);
		
		assertEquals(1, id.subIndex(5, 5).size());
		assertEquals(3, id.subIndex(5, 15).size());
		assertEquals(5, id.subIndex(4, 16).size());
		assertEquals(7, id.subIndex(0, 30).size());
		assertEquals(7, id.subIndex(0, 29).size());
		assertEquals(7, id.subIndex(1, 29).size());
		assertEquals(7, id.subIndex(-1, 31).size());
	}
	
	@Test
	public void testReadWrite() throws FileNotFoundException, IOException, ClassNotFoundException {
		MapFileIndex<Integer> id = new MapFileIndex<Integer>();
		
		for (int i = 0; i <= 30; i++) {
			if ((i % 5) == 0)
				id.createIndex(i, i);
			else
				id.skip();
		}
		
		File file = new File(UUID.randomUUID().toString());
		
		id.write(file);
		
		MapFileIndex<Integer> id2 = new MapFileIndex<Integer>();
		id2.read(file);
		
		int size = id2.size();
		assertEquals(7, size);
		
		for (int i = 0; i < size; i++) {
			MapFileIndex.Entry<Integer> entry = id2.getIndex(i);
			assertEquals(new Integer(i*5), entry.getKey());
			assertEquals(i == 0? 0 : 4, entry.getSkipped());
			assertEquals(5*i, entry.getOffset());
		}
		
		// Writing twice
		for (int i = 31; i <= 60; i++) {
			if ((i % 5) == 0)
				id.createIndex(i, i);
			else
				id.skip();
		}
		id.write(file);
		
		id2 = new MapFileIndex<Integer>();
		id2.read(file);
		
		size = id2.size();
		assertEquals(13, size);
		
		for (int i = 0; i < size; i++) {
			MapFileIndex.Entry<Integer> entry = id2.getIndex(i);
			assertEquals(new Integer(i*5), entry.getKey());
			assertEquals(i == 0? 0 : 4, entry.getSkipped());
			assertEquals(5*i, entry.getOffset());
		}
		
		file.delete();
	}
	
	@Test
	public void testDistance() {
		MapFileIndex<Integer> id = new MapFileIndex<Integer>();
		
		for (int i = 0; i < 30; i++) {
			if ((i % 5) == 0)
				id.createIndex(i, i);
			else
				id.skip();
		}
		
		assertEquals(6, id.size());
		
		assertEquals(0, id.distance(5, 5));
		assertEquals(5, id.distance(4, 4));
		assertEquals(5, id.distance(0, 5));
		assertEquals(10, id.distance(0, 6));
		assertEquals(25, id.distance(0, 30));
		assertEquals(5, id.distance(1, 2));
		assertEquals(10, id.distance(1, 6));
		assertEquals(10, id.distance(-1, 6));
		assertEquals(10, id.distance(1, 10));
		assertEquals(0, id.distance(100, 120));
		
	}

}
