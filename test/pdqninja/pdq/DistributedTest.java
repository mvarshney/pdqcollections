package pdqninja.pdq;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Map;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.PatternLayout;
import pdqninja.pdq.PDQ;
import pdqninja.pdq.PDQConfig;
import pdqninja.pdq.Parallel;
import pdqninja.pdqcollections.PDQMap;
import pdqninja.pdqcollections.TestSplittable;
import pdqninja.util.PrimitiveAdders;

public class DistributedTest implements Serializable{
	private static final long serialVersionUID = -4322643464765546004L;

	@Parallel(name="default")
	public void test(Integer i, Map<Integer, Integer> map) {
		System.out.println(String.format("Thread %d working on %d",
				Thread.currentThread().getId(), i));
		map.put(i, i);
	}
	
	
	public void test() throws Exception {
		PDQConfig conf = PDQConfig.current();
		conf.setMinFree(4L);
		conf.setSharedDir(conf.getLocalDir());
		conf.setWorkers("127.0.0.1:9760");
		
		TestSplittable<Integer> input = 
				new TestSplittable<Integer>(Arrays.asList(1, 2, 3, 4));
		
		Map<Integer, Integer> map = new PDQMap<Integer, Integer>(
				PrimitiveAdders.IntegerAdder);
		
		PDQ.run(this, input, map);
		
		//assertEquals(1, map.size());
//		assertEquals(Integer.valueOf(4), map.get(1));
		System.out.println(map);
		
		map.clear();
	}
	
	public static void main(String[] args) throws Exception {
		BasicConfigurator.configure(new ConsoleAppender(new PatternLayout("%r [%t] %-5p %c{1} %x - %m%n")));
		//Logger.getLogger("pdqninja.pdqcollections").setLevel(Level.ERROR);
		//Logger.getLogger("pdqninja").setLevel(Level.OFF);
		new DistributedTest().test();
	}
}
