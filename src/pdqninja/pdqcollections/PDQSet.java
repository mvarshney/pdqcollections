package pdqninja.pdqcollections;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import pdqninja.pdq.Mergeable;
import pdqninja.pdq.PDQConfig;
import pdqninja.pdq.Splittable;
import pdqninja.util.WrappedIterator;


public class PDQSet<E> implements Set<E>, Splittable, Mergeable, Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -8874226329962534843L;
	final List<Set<E>> delegates = new ArrayList<Set<E>>();
	final int partitions;
	Comparator<? super E> comparator;
	
	public PDQSet() {
		this.partitions = PDQConfig.current().getThreads();
		for (int i = 0; i < partitions; i ++) {
			delegates.add(newSet());
		}
	}
	
	public PDQSet(Comparator<? super E> comparator) {
		this();
		this.comparator = comparator;
	}
	
	public PDQSet(List<Set<E>> sets, Comparator<? super E> comparator) {
		this.partitions = sets.size();
		this.delegates.addAll(sets);
		this.comparator = comparator;
	}
	
	Set<E> newSet() {
		return new HashSet<E>();
	}
	
	Set<E> delegate(int hashcode) {
		return delegates.get(Math.abs(hashcode) % partitions);
	}
	
	@Override
	public Object replicate() {
		return new PDQSet<E>();
	}

	@Override
	public void merge(Object... objects) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public int size() {
		int size = 0;
		for (Collection<E> collection : delegates) {
			size += collection.size();
		}
		return size;
	}

	@Override
	public boolean isEmpty() {
		for (Collection<E> collection : delegates) {
			if (! collection.isEmpty())
				return false;
		}
		return true;
	}

	@Override
	public boolean contains(Object o) {
		return delegate(o.hashCode()).contains(o);
	}

	@Override
	public Iterator<E> iterator() {
		List<Iterator<E>> iterators = new ArrayList<Iterator<E>>();
		for (Collection<E> collection : delegates) {
			iterators.add(collection.iterator());
		}
		return new WrappedIterator<E>(iterators);
	}

	@Override
	public Object[] toArray() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T> T[] toArray(T[] a) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean add(E e) {
		return delegate(e.hashCode()).add(e);
	}

	@Override
	public boolean remove(Object o) {
		return delegate(o.hashCode()).remove(o);
	}

	@Override
	public boolean containsAll(Collection<?> c) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean addAll(Collection<? extends E> c) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean retainAll(Collection<?> c) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean removeAll(Collection<?> c) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void clear() {
		for (Collection<E> collection : delegates) {
			collection.clear();
		}
	}

	@SuppressWarnings("unchecked")
	int compare(E first, E second) {
		if (comparator != null) {
			return comparator.compare(first, second);
		} else {
			return ((Comparable<E>) first).compareTo(second);
		}
	}

	@Override
	public Iterator<?> getSplits() {
		// TODO Auto-generated method stub
		return null;
	}
}
