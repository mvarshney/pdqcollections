package pdqninja.pdqcollections;

import java.util.Comparator;

final class PQComparator<K, V> implements Comparator<PQEntry<K, V>> {
	private final Comparator<? super K> comparator;
	
	PQComparator(Comparator<? super K> comparator) { 
		this.comparator = comparator;
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public int compare(PQEntry<K, V> o1, PQEntry<K, V> o2) {
		if (comparator == null) {
			return ((Comparable<K>) o1.entry.getKey()).compareTo(o2.entry.getKey());
		} else {
			return comparator.compare(o1.entry.getKey(), o2.entry.getKey());
		}
	}
}