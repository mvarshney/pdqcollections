package pdqninja.util;

import static org.junit.Assert.*;

import org.junit.Test;

public class WeightedLoopIteratorTest {

	@Test
	public void testSimple() {
		Integer[] items = {10, 20, 30};
		int[] weights = {1, 2, 3};
		WeightedLoopIterator<Integer> it =
				new WeightedLoopIterator<Integer>(items, weights);
		
		Integer[] expected = {10, 20, 20, 30, 30, 30, 
				10, 20, 20, 30, 30, 30, 10};
		Integer[] obtained = new Integer[expected.length];
		
		for (int i = 0; i < expected.length; i++) {
			assertTrue(it.hasNext());
			obtained[i] = it.next();
		}	
		
		assertArrayEquals(expected, obtained);
	}

}
