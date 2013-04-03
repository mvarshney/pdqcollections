package pdqninja.util;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

import org.junit.Test;

public class WrappedIteratorTest {

	@Test
	public void testExhaustive() {
		Iterator<Integer> it1 = Arrays.asList(1, 2, 3, 4).iterator();
		Iterator<Integer> it2 = Arrays.asList(10, 20, 30).iterator();
		Iterator<Integer> it3 = Arrays.asList(100, 200, 300, 400, 500).iterator();
		Iterator<Integer> it4 = new ArrayList<Integer>().iterator();
		
		Integer[] expected = {1, 2, 3, 4, 10, 20, 30, 100, 200, 300, 400, 500};
		
		
		@SuppressWarnings("unchecked")
		WrappedIterator<Integer> it = new WrappedIterator<Integer>(
				Arrays.asList(it1, it4, it2, null, it3));
		
		for (int i = 0; i < expected.length; i++) {
			assertTrue(it.hasNext());
			assertEquals(expected[i], it.next());
		}
		assertFalse(it.hasNext());
		
	}
	
	@Test
	public void testSequential() {
		Iterator<Integer> it1 = Arrays.asList(1, 2, 3, 4).iterator();
		Iterator<Integer> it2 = Arrays.asList(10, 20, 30).iterator();
		Iterator<Integer> it3 = Arrays.asList(100, 200, 300, 400, 500).iterator();
		Iterator<Integer> it4 = new ArrayList<Integer>().iterator();
		
		Integer[] expected = {1, 10, 100,  2, 20, 200, 3, 30, 300, 4, 400, 500};
		
		
		@SuppressWarnings("unchecked")
		WrappedIterator<Integer> it = new WrappedIterator<Integer>(
				Arrays.asList(it1, it4, it2, null, it3), false);
		
		for (int i = 0; i < expected.length; i++) {
			assertTrue(it.hasNext());
			assertEquals(expected[i], it.next());
		}
		assertFalse(it.hasNext());
		
	}

}
