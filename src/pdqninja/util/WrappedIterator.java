package pdqninja.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 * Wraps a collection of Iterators to present a view
 * of single Iterator.
 *  
 * @author mvarshney
 */
public class WrappedIterator<E> implements Iterator<E> {
	private final List<Iterator<E>> allIterators = new ArrayList<Iterator<E>>();
	private int lastIterator = -1;
	private int nextIterator = 0;
	private boolean exhaustive = true;
	
	/**
	 * Constructs an Iterator that iterates sequentially
	 * over multiple iterators.
	 * @param iterators the iterators over which this
	 * object is to iterate
	 */
	public WrappedIterator(Collection<? extends Iterator<E>> iterators) {
		this(iterators, true);
	}
	
	public WrappedIterator(Collection<? extends Iterator<E>> iterators, boolean exhaustive) {
		this.exhaustive = exhaustive;
		allIterators.addAll(iterators);
		
		moveNext();
	}
	
	/**
	 * Read the next available item.
	 */
	void moveNext() {
		
		int size = allIterators.size();
		for (int i = 0; i < size; i++, nextIterator++) {
			nextIterator = nextIterator % size;
			Iterator<E> current = allIterators.get(nextIterator);
			if (current != null && current.hasNext()) {
				return;
			}
		}
		
		nextIterator = -1;
	}

	@Override
	public boolean hasNext() {
		return nextIterator != -1;
	}

	@Override
	public E next() {
		Iterator<E> it = allIterators.get(nextIterator);
		E value = it.next();
		lastIterator = nextIterator;
		if (! exhaustive) nextIterator ++;
		
		moveNext();
		return value;
	}

	@Override
	public void remove() {
		if (lastIterator == -1) 
			throw new IllegalStateException();
		Iterator<E> it = allIterators.get(lastIterator);
		if (it != null) it.remove();
	}
}
