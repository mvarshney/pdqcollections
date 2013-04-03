package pdqninja.pdqcollections;

import static org.junit.Assert.*;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Map;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import pdqninja.pdq.PDQ;
import pdqninja.pdq.PDQConfig;
import pdqninja.pdq.Parallel;
import pdqninja.util.PrimitiveAdders;

public class PDQMapTest {

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		BasicConfigurator.configure(new ConsoleAppender(new PatternLayout("%r [%t] %-5p %c{1} %x - %m%n")));
		//Logger.getLogger("pdqninja.pdqcollections").setLevel(Level.ERROR);
		Logger.getLogger("pdqninja").setLevel(Level.OFF);
	}
	
	@Parallel(name="addone")
	public void addone(Integer i, Map<Integer, Integer> map) {
		map.put(i, i);
	}
	
	
	
	void doSimple() throws NoSuchMethodException, RuntimeException, IllegalAccessException, InvocationTargetException, InterruptedException, IOException {
		PDQConfig.current().setThreads(2);
		
		TestSplittable<Integer> input = 
				new TestSplittable<Integer>(Arrays.asList(1, 1, 1, 1));
		
		Map<Integer, Integer> map = new PDQMap<Integer, Integer>(
				PrimitiveAdders.IntegerAdder);
		
		PDQ.run(new PDQMapTest(), "addone", input, map);
		
		assertEquals(1, map.size());
		assertEquals(Integer.valueOf(4), map.get(1));
	}
	
	@Test
	public void testSimpleMemory() throws IOException, NoSuchMethodException, RuntimeException, IllegalAccessException, InvocationTargetException, InterruptedException {
		PDQConfig.current().setThreads(2);
		PDQConfig.current().setMinFree(4L);
		doSimple();
	}
	
	@Test
	public void testSimpleDisk() throws IOException, NoSuchMethodException, RuntimeException, IllegalAccessException, InvocationTargetException, InterruptedException {
		PDQConfig.current().setThreads(2);
		PDQConfig.current().setMinFree(1024L * 1024 * 1024);
		
		doSimple();
	}
	
	@Test
	public void testSimpleDistributedMemory() throws Exception {
		PDQConfig.current().setMinFree(4L);
		PDQConfig.current().setSharedDir(PDQConfig.current().getLocalDir());
		PDQConfig.current().setWorkers("127.0.0.1:9760");
		doSimple();
		PDQConfig.current().setWorkers(null);
	}
	
	@Test
	public void testSimpleDistributedDisk() throws Exception {
		PDQConfig.current().setMinFree(1024L * 1024 * 1024);
		PDQConfig.current().setSharedDir(PDQConfig.current().getLocalDir());
		PDQConfig.current().setWorkers("127.0.0.1:9760");
		doSimple();
		PDQConfig.current().setWorkers(null);
	}
	
	@Parallel(name="addmany")
	public void addmany(Integer i, Map<Integer, Integer> map) {
		int len = i.intValue();
		for (int j = 0; j < len; j++)
			map.put(j, 1);
	}
	
	@Parallel(name="copymap")
	public void copymap(Map<Integer, Integer> from,
			Map<Integer, Integer> to) {
		
		for (Map.Entry<Integer, Integer> entry: from.entrySet()) {
			to.put(entry.getKey(), entry.getValue() * 2);
		}
	}

	void doTwoStage() throws NoSuchMethodException, RuntimeException, IllegalAccessException, InvocationTargetException, InterruptedException, IOException {
		TestSplittable<Integer> input = 
				new TestSplittable<Integer>(Arrays.asList(1000, 1000, 1000, 1000));
		
		Map<Integer, Integer> map = new PDQMap<Integer, Integer>(
				PrimitiveAdders.IntegerAdder);
		
		PDQ.run(new PDQMapTest(), "addmany", input, map);
		
		assertEquals(1000, map.size());
		assertEquals(Integer.valueOf(4), map.get(2));
		assertEquals(Integer.valueOf(4), map.get(1));
		
		for (int i = 999; i >= 0; i--)
			assertEquals(""+ i, Integer.valueOf(4), map.get(i));
		
		assertNull(map.get(1000));
		assertNull(map.get(-1));
			
		
		// Use this map as input now
		Map<Integer, Integer> map2 = new PDQMap<Integer, Integer>(
				PrimitiveAdders.IntegerAdder);
		
		PDQ.run(new PDQMapTest(), "copymap", map, map2);
		
		assertEquals(1000, map2.size());
		assertEquals(Integer.valueOf(8), map2.get(2));
		assertEquals(Integer.valueOf(8), map2.get(1));
		
		for (int i = 999; i >= 0; i--)
			assertEquals(""+ i, Integer.valueOf(8), map2.get(i));
		
		assertNull(map2.get(1000));
		assertNull(map2.get(-1));
		
		
		map.clear();
		map2.clear();
	}
	
	@Test
	public void testTwoStageMemory() throws Exception {
		PDQConfig.current().setThreads(2);
		PDQConfig.current().setMinFree(4L);
		doTwoStage();
	}
	
	@Test
	public void testTwoStageDisk() throws Exception {
		PDQConfig.current().setThreads(2);
		PDQConfig.current().setMinFree(1024L * 1024 * 1024);
		PDQConfig.current().setBlocksize(4L * 1024);
		PDQConfig.current().setExternalIndex(5L * 1024);
		
		doTwoStage();
	}
	
	@Test
	public void testTwoStageDistributedMemory() throws Exception {
		PDQConfig.current().setMinFree(4L);
		PDQConfig.current().setSharedDir(PDQConfig.current().getLocalDir());
		PDQConfig.current().setWorkers("127.0.0.1:9760");
		doTwoStage();
		PDQConfig.current().setWorkers(null);
	}
	
	@Test
	public void testTwoStageDistributedDisk() throws Exception {
		PDQConfig.current().setMinFree(1024L * 1024 * 1024);
		PDQConfig.current().setSharedDir(PDQConfig.current().getLocalDir());
		PDQConfig.current().setWorkers("127.0.0.1:9760");
		doTwoStage();
		PDQConfig.current().setWorkers(null);
	}
	
	@Parallel(name="self")
	public void self(Map<Integer, Integer> from, Map<Integer, Integer> to) throws Exception {
		for (Map.Entry<Integer, Integer> entry: from.entrySet()) {
			to.put(entry.getKey(), 2 * entry.getValue());
		}
	}
	
	void doSelf() throws Exception {
		Map<Integer, Integer> map = new PDQMap<Integer, Integer>(
				PrimitiveAdders.IntegerAdder);
		for (int i = 0; i < 100; i++)
			map.put(i, 1);
		
		PDQ.run(this, "self", map, map);
		
		assertEquals(100, map.size());
		for (int i = 0; i < 100; i++)
			assertEquals(Integer.valueOf(3), map.get(i));
	}
	@Ignore
	@Test
	public void testSelfMemory() throws Exception {
		PDQConfig.current().setThreads(2);
		PDQConfig.current().setMinFree(4L);
		doSelf();
	}
	@Ignore
	@Test
	public void testSelfDisk() throws Exception {
		PDQConfig.current().setThreads(2);
		PDQConfig.current().setMinFree(1024L * 1024 * 1024);
		doSelf();
	}
	
	@Ignore
	@Test
	public void testSelfDistributedMemory() throws Exception {
		PDQConfig.current().setMinFree(4L);
		PDQConfig.current().setSharedDir(PDQConfig.current().getLocalDir());
		PDQConfig.current().setWorkers("127.0.0.1:9760");
		doSelf();
		PDQConfig.current().setWorkers(null);
	}
	@Ignore
	@Test
	public void testTwoSelfDistributedDisk() throws Exception {
		PDQConfig.current().setMinFree(1024L * 1024 * 1024);
		PDQConfig.current().setSharedDir(PDQConfig.current().getLocalDir());
		PDQConfig.current().setWorkers("127.0.0.1:9760");
		doSelf();
		PDQConfig.current().setWorkers(null);
	}
}

