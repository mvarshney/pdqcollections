package pdqninja.util;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

public class WrappedContainerTest {

	@SuppressWarnings("unchecked")
	@Test
	public void testWrappedIterator() {
		List<Integer> a1 = Arrays.asList(1, 11, 3);
		List<Integer> a2 = Arrays.asList(2, 20, 4, 40);
		
		WrappedIterator<Integer> wi =
				new WrappedIterator<Integer>(
						Arrays.asList(a1.iterator(), a2.iterator()));
		
		Integer[] result = new Integer[7];
		for (int i = 0; i < 7; i++) {
			assertTrue(wi.hasNext());
			result[i] = wi.next();
		}
		assertFalse(wi.hasNext());
		
		Integer[] expected = {1, 11, 3, 2, 20, 4, 40};
		assertArrayEquals(expected, result);
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testWrappedSortedIterator() {
		List<Integer> a1 = Arrays.asList(3, 11, 1);
		List<Integer> a2 = Arrays.asList(2, 0, 20, 4);
		
		
		WrappedSortedIterator<Integer> wsi = 
				new WrappedSortedIterator<Integer>(
						Arrays.asList(a1.iterator(), a2.iterator()));
		
		
		
		Integer[] result = new Integer[7];
		for (int i = 0; i < 7; i++) {
			assertTrue(wsi.hasNext());
			result[i] = wsi.next();
		}
		assertFalse(wsi.hasNext());
		
		Integer[] expected = {2, 0, 3, 11, 1, 20, 4};
		assertArrayEquals(expected, result);
	
		
	}

}
