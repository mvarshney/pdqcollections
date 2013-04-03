package pdqninja.util;

import java.util.Iterator;

public class FireOnceIterator<E> implements Iterator<E> {
	E item;
	
	public FireOnceIterator(E item) {
		this.item = item;
	}
	
	@Override
	public boolean hasNext() {
		return item != null;
	}

	@Override
	public E next() {
		E val = item;
		item = null;
		return val;
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}

}
