package pdqninja.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 * Wraps multiple collections to present a view of single
 * collection. This is an immutable class - any
 * operation that attempts to modify the collection will
 * result in <code>UnsupportedOperationException</code>.
 * 
 * 
 * @author mvarshney
 *
 */
public class WrappedCollection<E> implements Collection<E> {
	private final Collection<? extends Collection<E>> delegates;
	
	/**
	 * Constructs the object that will wrap collections.
	 * 
	 * @param collections
	 */
	public WrappedCollection(Collection<? extends Collection<E>> collections) {
		this.delegates = collections;
	}
	
	/**
	 * Returns the size. This is equal to the sum of the sizes
	 * of all collections.
	 */
	@Override
	public int size() {
		int size = 0;
		for (Collection<E> collection : delegates) {
			size += collection.size();
		}
		return size;
	}

	/**
	 * Returns <code>true</code> if all the collections
	 * are empty;
	 */
	@Override
	public boolean isEmpty() {
		for (Collection<E> collection : delegates) {
			if (! collection.isEmpty())
				return false;
		}
		return true;
	}

	/**
	 * Return <code>true</code> if any of the collections 
	 * contain the value.
	 */
	@Override
	public boolean contains(Object o) {
		for (Collection<E> collection : delegates) {
			if (collection.contains(o))
				return true;
		}
		return false;
	}
	
	/**
	 * Returns an iterator that iterates over all values
	 * in all the collections.
	 */
	@Override
	public Iterator<E> iterator() {
		List<Iterator<E>> iterators = new ArrayList<Iterator<E>>();
		for (Collection<E> collection : delegates) {
			iterators.add(collection.iterator());
		}
		return new WrappedIterator<E>(iterators);
	}

	/**
	 * 
	 */
	@Override
	public Object[] toArray() {
		int size = size(); 
		return toArray(new Object[size]);
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> T[] toArray(T[] a) {
		int size = size();
		if (a.length < size) {
			// TODO: 
			a = (T[]) java.lang.reflect.Array
	                  .newInstance(a.getClass().getComponentType(), size);
		}
		Iterator<E> it = iterator();
		int idx = 0;
		while (it.hasNext()) {
			a[idx++] = (T) it.next();
		}
		return a;
	}

	@Override
	public boolean add(E e) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean remove(Object o) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean containsAll(Collection<?> c) {
		Iterator<?> e = c.iterator();
		while (e.hasNext())
		    if (!contains(e.next()))
		    	return false;
		return true;
	}

	@Override
	public boolean addAll(Collection<? extends E> c) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean removeAll(Collection<?> c) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean retainAll(Collection<?> c) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void clear() {
		throw new UnsupportedOperationException();
	}

}
