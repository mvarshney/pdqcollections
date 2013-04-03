package pdqninja.pdqcollections;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;
import java.util.UUID;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.junit.BeforeClass;
import org.junit.Test;

import pdqninja.pdq.PDQConfig;
import pdqninja.util.SortedMultimap;

public class ExternalSortedMultimapTest {
	File file;
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		BasicConfigurator.configure(new ConsoleAppender(new PatternLayout("%r [%t] %-5p %c{1} %x - %m%n")));
		//Logger.getLogger("pdqninja.pdqcollections").setLevel(Level.ERROR);
		Logger.getLogger("pdqninja").setLevel(Level.OFF);
	}
	
	ExternalSortedMultimap<Integer, Integer> create() throws IOException {
		file = new File(UUID.randomUUID().toString());
		file.createNewFile();
		file.deleteOnExit();
		
		ExternalSortedMultimap<Integer, Integer> map =
				new ExternalSortedMultimap<Integer, Integer>(file, null, true);
		
		Integer[] ints = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
		
		for (int i = 0; i < 10; i++)
			map.put(i, Arrays.asList(Arrays.copyOf(ints, i)));

		return map;
	}
	
	void delete() {
		file.delete();
	}
	
	@Test
	public void testReadWrite() throws IOException {
		ExternalSortedMultimap<Integer, Integer> map = create();
		map.flush();
		
		int count = 0;
		for (Integer i: map.keySet()) {
			assertEquals(Integer.valueOf(count), i);
			count ++;
		}
		assertEquals(10, count);
		
		count = 0;
		for (Map.Entry<Integer, Collection<Integer>> entry: map.allEntrySet()) {
			assertEquals(Integer.valueOf(count), entry.getKey());
			assertEquals(count, entry.getValue().size());
			count ++;
		}
		assertEquals(10, count);
		
		delete();
	}
	
	@Test
	public void testSubmaps() throws IOException {
		ExternalSortedMultimap<Integer, Integer> map = create();
		map.flush();
		
		map.close();
		
		verifyMap(map.headMap(4), 0, 4);
		verifyMap(map.headMap(11), 0, 10);
		verifyMap(map.headMap(0), 0, 0);
		assertNull(map.headMap(-1));
		
		verifyMap(map.tailMap(5), 5, 10);
		verifyMap(map.tailMap(0), 0, 10);
		assertNull(map.tailMap(11));
		
		verifyMap(map.subMap(-4, 16), 0, 10);
		verifyMap(map.subMap(-4, 6), 0, 6);
		verifyMap(map.subMap(4, 16), 4, 10);
		verifyMap(map.subMap(4, 7), 4, 7);
		verifyMap(map.subMap(4, 4), 0, 0);
		
		file.delete();
	}
	
	@Test
	public void testGet() throws IOException {

		PDQConfig.current().setBlocksize(4);
		PDQConfig.current().setExternalIndex(4);
		
		ExternalSortedMultimap<Integer, Integer> map = create();
		map.flush();
		
		
		for (int i = 0; i < 10; i++) {
			assertEquals(i, map.getAll(i).size());
		}
		
		assertNull(map.getAll(11));
		assertNull(map.getAll(-1));
		
		
		map.close();
		
		file.delete();
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testSplit() throws IOException {
		PDQConfig.current().setBlocksize(4);
		PDQConfig.current().setExternalIndex(4);
		
		ExternalSortedMultimap<Integer, Integer> map = create();
		map.flush();
		map.close();
		
		Iterator<SortedMultimap<Integer, Integer>> splits = 
				(Iterator<SortedMultimap<Integer, Integer>>) map.getSplits();
		
		for (int i = 0; i < 9; i++) {
			assertTrue(splits.hasNext());
			SortedMultimap<Integer, Integer> m = splits.next();
			verifyMap(m, i, i == 8 ? 10 : i + 1);	
		}
		assertFalse(splits.hasNext());
		
		// do it again
		splits = (Iterator<SortedMultimap<Integer, Integer>>) map.getSplits();
		
		for (int i = 0; i < 9; i++) {
			assertTrue(splits.hasNext());
			SortedMultimap<Integer, Integer> m = splits.next();
			verifyMap(m, i, i == 8 ? 10 : i + 1 );	
		}
		assertFalse(splits.hasNext());
		
		map.close();
		file.delete();
	}

	void verifyMap(SortedMap<Integer, Integer> map, int from, int to) {
		int count = from;
		for (Integer key: map.keySet()) {
			assertEquals(Integer.valueOf(count), key);
			count ++;
		}
		assertEquals(to, count);
		
	}
}
