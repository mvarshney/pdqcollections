package pdqninja.pdqcollections;

import java.io.Serializable;
import java.util.Collection;
import java.util.Iterator;

import pdqninja.pdq.Splittable;

public class TestSplittable<E> implements Splittable, Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 4214260809194396949L;
	Collection<? extends E> collection;
	
	public TestSplittable(Collection<? extends E> collection) {
		this.collection = collection;
	}
	
	@Override
	public Iterator<?> getSplits() {
		return collection.iterator();
	}

}
