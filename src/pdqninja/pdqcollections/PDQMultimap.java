package pdqninja.pdqcollections;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import pdqninja.pdq.PDQ;
import pdqninja.util.HashMultimap;
import pdqninja.util.Multimap;
import pdqninja.util.WrappedSortedIterator;


public class PDQMultimap<K, V> extends PDQMap<K, V> implements Multimap<K, V> {
	/**
	 * 
	 */
	private static final long serialVersionUID = 7703739885486219314L;
	transient Set<Map.Entry<K, Collection<V>>> allEntrySet;
	
	public PDQMultimap() {
		super();
	}
		
	public PDQMultimap(Comparator<? super K> comparator) {
		super(comparator);
	}

	public PDQMultimap(List<Map<K, V>> maps, Comparator<? super K> comparator) {
		super(maps, comparator, null);
	}

	@Override
	protected Map<K, V> createDelegate() {
		return new HashMultimap<K, V>();
	}
	
	@Override
	protected boolean isMultimap() {
		return true;
	}
	
	@Override
	ExternalSortedMap<K, V> newExternalMap(boolean isIntermediate) throws IOException {
		boolean indexed = false;
		if ((PDQ.rank() == 0) && !isIntermediate)
			indexed = true;
		
		return new ExternalSortedMultimap<K, V>(
				newExternalMapFile(isIntermediate),
				comparator,
				indexed);
	}
	
	Multimap<K, V> multimap(int hashcode) {
		return (Multimap<K, V>) delegates.get(Math.abs(hashcode) % partitions);
	}
	
	@Override
	public void put(K key, Collection<? extends V> values) {
		multimap(key.hashCode()).put(key, values);
	}

	@Override
	public boolean contains(K key, V value) {
		return multimap(key.hashCode()).contains(key, value);
	}

	@Override
	public boolean remove(Object key, Object value) {
		return multimap(key.hashCode()).remove(key, value);
	}

	final class AllEntryComparator implements Comparator<Map.Entry<K, Collection<V>>>, Serializable {
		private static final long serialVersionUID = -2812120194529668320L;

		@Override
		public int compare(Map.Entry<K, Collection<V>> o1, Map.Entry<K, Collection<V>> o2) {
			return PDQMultimap.this.compare(o1.getKey(), o2.getKey());
		}
	}
	
	final AllEntryComparator allEntryComparator = new AllEntryComparator();
	
	@Override
	public Set<Map.Entry<K, Collection<V>>> allEntrySet() {
		if (allEntrySet == null) {
			List<Set<Map.Entry<K, Collection<V>>>> entrySets = 
					new ArrayList<Set<Map.Entry<K, Collection<V>>>>(partitions);
			for (Map<K, V> map : delegates) {
				Multimap<K, V> mmap = (Multimap<K, V>) map;
				entrySets.add(mmap.allEntrySet());
			}
			if (isSorted()) {
				allEntrySet = new PDQSet<Map.Entry<K, Collection<V>>>(entrySets, allEntryComparator) {
					/**
					 * 
					 */
					private static final long serialVersionUID = 2122116740351353500L;

					@Override
					public Iterator<Map.Entry<K, Collection<V>>> iterator() {
						List<Iterator<Map.Entry<K, Collection<V>>>> iterators = 
								new ArrayList<Iterator<Map.Entry<K, Collection<V>>>>();
						for (Collection<Map.Entry<K, Collection<V>>> collection : delegates) {
							iterators.add(collection.iterator());
						}
						return new WrappedSortedIterator<Map.Entry<K, Collection<V>>>(iterators, comparator);
					}
				};
			} else {
				allEntrySet = new PDQSet<Map.Entry<K, Collection<V>>>(entrySets, null);
			}
		}
		return allEntrySet;
	}

	@Override
	public Collection<V> getAll(Object key) {
		return multimap(key.hashCode()).getAll(key);
	}
	
	@Override
	public Object replicate() {
		return new PDQMultimap<K, V>();
	}
}
