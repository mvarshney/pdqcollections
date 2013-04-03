package pdqninja.util;

import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Iterate in a loop over an array of objects, in ratios 
 * determined by their specified weight.
 * <p>
 * As an example, for a collection of items [Item1, Item2, Item3]
 * with weights [2, 1, 3], the <code>WeightedLoopIterator</code>
 * will iterate as follows:
 * <p>
 * Item1, Item1, Item2, Item3, Item3, Item3, Item1, Item1, Item2, ...
 * <p>
 * Notice the first six entries - the items are returned in
 * ratio of their relative weights. Also notice that the pattern
 * repeats itself indefinitely, that is, {@link #hasNext()}
 * will always return <code>true</code>.
 * <p>
 * This iterator allows items to be removed, in which case the
 * weight of the removed item will be set to zero. Only when all items
 * are removed (or equivalently, when the sum of weights is zero),
 * {@link #hasNext()} will return false.
 * <p>
 * {@link #cycle()} methods returns the periodicity of the loop,
 * which is equal to the sum of the weights of the items.  
 * 
 * @author mvarshney
 */
public class WeightedLoopIterator<E> implements Iterator<E> {
	private final E[] items;
	private final int[] weights;
	
	/**
	 * Counts, in a loop, from 1 to sum to weights.
	 */
	private int counter = 1;
	
	/**
	 *  Array index that is returned by next()
	 */
	private int cursor = -1;
	
	/** 
	 * Array index returned by last call to next().
	 * Is reset to -1 after remove()
	 */
	private int lastIndex = -1;
	
	/**
	 * Creates a WeightedLoopIterator for the given items
	 * where each item has equal weight.
	 * @param items the items that are to be iterated in a loop
	 */
	public WeightedLoopIterator(E[] items) {
		this(items, null);
	}
	
	/**
	 * Creates a WeightedLoopIterator for the given items
	 * where each item has a specified weight.
	 * @param items the items that are to iterated in a loop
	 * @param weights corresponding weight of each item
	 */
	public WeightedLoopIterator(E[] items, int[] weights) {
		if (items == null) 
			throw new IllegalArgumentException("Items array is null");
		
		if (weights != null && (items.length != weights.length))
			throw new IllegalArgumentException("Sizes of items array " +
					"and weights array are not same");
		
		int len = items.length;
		
		this.items = Arrays.copyOf(items, len);
		this.weights = new int[len];
		
		// Create weights table. Weight is zero if item is null,
		// is one if not defined, otherwise equal to specified 
		for (int i = 0; i < len; i++) {
			if (items[i] != null) {
				// this is this first item returned by next()
				if (cursor == -1) cursor = i;
				
				int weight = (weights == null) ? 1 : weights[i];
				this.weights[i] = weight;
			} else {
				this.weights[i] = 0;
			}
		}
		
		// Create a cumulative sum of weights.
		for (int i = 1; i < len; i++) {
			this.weights[i] += this.weights[i - 1];
		}
	}
	
	@Override
	public boolean hasNext() {
		// Recall that <code>weights</code> is a cumulative
		// array of weights. If the last element is zero,
		// it implies there are no items to iterate over.
		if (items == null || items.length == 0)
			return false;
		return weights[weights.length - 1] != 0;
	}

	@Override
	public E next() {
		if (cursor == -1)
			throw new NoSuchElementException();
		
		E next = items[cursor];
		lastIndex = cursor;
		
		// locate the next cursor location.
		// skip through those array indices that have null items
		// (or equivalently: have zero weights)
		int len = items.length;
		while (true) {
			counter ++;
			if (counter > weights[cursor]) {
				cursor ++;
				if (cursor == len) {
					cursor = 0;
					counter = 1;
				}
			}
			
			if (weights[cursor] != 0)
				break;
		}

		
		return next;
	}

	@Override
	public void remove() {
		if (lastIndex == -1)
			throw new IllegalStateException();

		int lastWeight = weights[lastIndex];
		
		// Remove item and set its weight to 0
		items[lastIndex] = null;
		weights[lastIndex] = 0;
		
		// Adjust cumulative weights
		for (int i = lastIndex + 1; i < weights.length; i++) {
			weights[i] -= lastWeight;
		}
		
		// reset the lastIndex
		lastIndex = -1;
	}
	
	/**
	 * Returns the iteration loop length, which is equal
	 * to the sum of the weights of the items.
	 * @return iteration loop length
	 */
	public int cycle() {
		if (items == null || items.length == 0)
			return 0;
		return weights[weights.length - 1];
	}
	
	/**
	 * Returns the item that was returned in the last
	 * call to {@link #next()}.
	 *  
	 * @return the item that was returned in the last
	 * call to {@link #next()}
	 */
	protected E last() {
		if (lastIndex == -1)
			throw new IllegalStateException();
	
		return items[lastIndex];
	}

}
