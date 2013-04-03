package pdqninja.util;

import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.PriorityQueue;

/**
 * Wraps a collected of sorted iterators to present a
 * view of a single sorted iterator.
 * 
 * @author mvarshney
 */
public class WrappedSortedIterator<E> implements Iterator<E> {
	/**
	 * Objects of this class are stored in the priority queue.
	 */
	private static final class Entry<E> {
		E data;
		Iterator<E> iterator;
		
		public Entry(E data, Iterator<E> iterator) {
			this.data = data;
			this.iterator = iterator;
		}
	}
	
	/**
	 * Defines the comparator for the {@link Entry} objects.
	 */
	private static class EntryComparator<E> implements Comparator<Entry<E>> {
		private final Comparator<? super E> comparator;
		
		EntryComparator(Comparator<? super E> comparator) {
			this.comparator = comparator;
		}
		
		@SuppressWarnings("unchecked")
		@Override
		public int compare(Entry<E> o1, Entry<E> o2) {
			if (comparator == null)
				return ((Comparable<E>) o1.data).compareTo(o2.data);
			else
				return comparator.compare(o1.data, o2.data); 
		}

	}

	private final PriorityQueue<Entry<E>> pqueue;
	private Entry<E> current;
	private E next;

	public WrappedSortedIterator(Collection<? extends Iterator<E>> iterators) {
		this(iterators, null);
	}

	public WrappedSortedIterator(Collection<? extends Iterator<E>> iterators,
			Comparator<? super E> comparator) {
		
		EntryComparator<E> comp = new EntryComparator<E>(comparator);
		pqueue = new PriorityQueue<Entry<E>>(iterators.size(), comp);
		
		for (Iterator<E> it: iterators) {
			if (it.hasNext()) {
				pqueue.add(new Entry<E>(it.next(), it));
			}
		}
		
		readNext();
	}
	
	void readNext() {
		if (pqueue.size() == 0) {
			next = null;
			current = null;
			return;
		}
		// get the iterator with smallest entry
		current = pqueue.poll();
		
		next = current.data;
		
		// if the iterator has more items remaining, retreive
		// the next one and put the entry back in the pqueue
		if (current.iterator.hasNext()) {
			current.data = current.iterator.next();
			pqueue.add(current);
		}
	}

	@Override
	public boolean hasNext() {
		return next != null;
	}

	@Override
	public E next() {
		E value = next;
		readNext();
		return value;
	}

	@Override
	public void remove() {
		current.iterator.remove();
	}
}
