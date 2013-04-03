package pdqninja.pdqcollections;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
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
import org.junit.Ignore;
import org.junit.Test;

import pdqninja.pdq.PDQConfig;

public class ExternalMapTest {

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		BasicConfigurator.configure(new ConsoleAppender(new PatternLayout("%r [%t] %-5p %c{1} %x - %m%n")));
		//Logger.getLogger("pdqninja.pdqcollections").setLevel(Level.ERROR);
		Logger.getLogger("pdqninja").setLevel(Level.OFF);
	}
	
	@Test
	public void testReadWrite() throws IOException {
		File file = new File(UUID.randomUUID().toString());
		file.createNewFile();
		
		ExternalSortedMap<String, String> map =
				new ExternalSortedMap<String, String>(file, null, false);
		
		for (int i = 0; i < 10; i++)
			map.put("key" + i, "value" + i);
		
		map.close();
		
		int count = 0;
		for (Map.Entry<String, String> entry: map.entrySet()) {
			assertEquals(entry.getKey(), "key" + count);
			assertEquals(entry.getValue(), "value" + count);
			count ++;
		}
		assertEquals(10, count);
		
		count = 0;
		for (String s: map.keySet()) {
			assertEquals(s, "key" + count);
			count ++;
		}
		assertEquals(10, count);
		
		count = 0;
		for (String s: map.values()) {
			assertEquals(s, "value" + count);
			count ++;
		}
		assertEquals(10, count);
		
		file.delete();
	}
	
	@Ignore
	@Test
	public void testSubmaps() throws IOException {
		File file = new File(UUID.randomUUID().toString());
		file.createNewFile();
		
		ExternalSortedMap<Integer, String> map =
				new ExternalSortedMap<Integer, String>(file, null, false);
		
		for (int i = 0; i < 10; i++)
			map.put(i, "value" + i);
		
		map.close();
		
		verifyMap(map.headMap(4), 0, 4);
		verifyMap(map.headMap(11), 0, 10);
		verifyMap(map.headMap(0), 0, 0);
		assertNull(map.headMap(-1));
		//verifyMap(map.headMap(-1), 0, 0);
		
		verifyMap(map.tailMap(5), 5, 10);
		verifyMap(map.tailMap(0), 0, 10);
		//verifyMap(map.tailMap(11), 0, 0);
		assertNull(map.tailMap(11));
		
		verifyMap(map.subMap(-4, 16), 0, 10);
		verifyMap(map.subMap(-4, 6), 0, 6);
		verifyMap(map.subMap(4, 16), 4, 10);
		verifyMap(map.subMap(4, 7), 4, 7);
		verifyMap(map.subMap(4, 4), 0, 0);
		
		file.delete();
	}
	
	void verifyMap(SortedMap<Integer, String> map, int from, int to) {
		int count = from;
		for (Integer key: map.keySet()) {
			assertEquals(Integer.valueOf(count), key);
			count ++;
		}
		assertEquals(to, count);
		
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testSplit() throws IOException {
		File file = new File(UUID.randomUUID().toString());
		file.createNewFile();
		
		PDQConfig.current().setBlocksize(4);
		PDQConfig.current().setExternalIndex(4);
		
		ExternalSortedMap<Integer, String> map =
				new ExternalSortedMap<Integer, String>(file, null, true);
		
		for (int i = 0; i < 10; i++)
			map.put(i, "value" + i);
		
		map.close();
		
		System.out.println(map.index.size());
		
		Iterator<SortedMap<Integer, String>> splits = 
				(Iterator<SortedMap<Integer, String>>) map.getSplits();
		
		
		for (int i = 0; i < 9; i++) {
			System.out.println("Loop value is " + i);
			assertTrue(splits.hasNext());
			SortedMap<Integer, String> m = splits.next();
			verifyMap(m, i, i == 8 ? 10 : i + 1);	
		}
		assertFalse(splits.hasNext());
		
		// do it again
		splits = (Iterator<SortedMap<Integer, String>>) map.getSplits();
		
		for (int i = 0; i < 9; i++) {
			assertTrue(splits.hasNext());
			SortedMap<Integer, String> m = splits.next();
			verifyMap(m, i, i == 8 ? 10 : i + 1 );	
		}
		assertFalse(splits.hasNext());
	}

	
	@Test
	public void testGet() throws IOException {
		File file = new File(UUID.randomUUID().toString());
		file.createNewFile();
		
		PDQConfig.current().setBlocksize(4);
		PDQConfig.current().setExternalIndex(4);
		
		ExternalSortedMap<Integer, String> map =
				new ExternalSortedMap<Integer, String>(file, null, true);
		
		for (int i = 0; i < 10; i++)
			map.put(i, "value" + i);
		
		map.flush();
		
		assertEquals("value1", map.get(1));
		assertEquals("value9", map.get(9));
		assertEquals("value2", map.get(2));
		assertEquals("value3", map.get(3));
		assertEquals("value0", map.get(0));
		assertNull(map.get(11));
		assertNull(map.get(-1));
		
		
		map.close();
		
		file.delete();
	}
}
