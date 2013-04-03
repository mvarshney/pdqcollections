package pdqninja.pdqcollections;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

import pdqninja.util.SortedMultimap;
import pdqninja.util.TreeMultimap;

public class PDQSortedMultimap<K, V> extends PDQMultimap<K, V> implements SortedMultimap<K, V> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -2059843306504205813L;

	public PDQSortedMultimap() {
		super();
	}


	public PDQSortedMultimap(Comparator<? super K> comparator) {
		super(comparator);
	}


	public PDQSortedMultimap(List<Map<K, V>> maps,
			Comparator<? super K> comparator) {
		super(maps, comparator);
	}
	
	@Override
	protected boolean isSorted() {
		return true;
	}

	@Override
	protected Map<K, V> createDelegate() {
		return new TreeMultimap<K, V>();
	}
	
	@Override
	public Comparator<? super K> comparator() {
		return comparator;
	}
	

	@Override
	public SortedMultimap<K, V> subMap(K fromKey, K toKey) {
		List<Map<K, V>> maps = new ArrayList<Map<K, V>>();
		for (Map<K, V> map: delegates) {
			SortedMultimap<K, V> smap = (SortedMultimap<K, V>) map;
			maps.add(smap.subMap(fromKey, toKey));
		}
		
		return new PDQSortedMultimap<K, V>(maps, comparator);
	}

	@Override
	public SortedMultimap<K, V> headMap(K toKey) {
		List<Map<K, V>> maps = new ArrayList<Map<K, V>>();
		for (Map<K, V> map: delegates) {
			SortedMultimap<K, V> smap = (SortedMultimap<K, V>) map;
			maps.add(smap.headMap(toKey));
		}
		
		return new PDQSortedMultimap<K, V>(maps, comparator);
	}

	@Override
	public SortedMultimap<K, V> tailMap(K fromKey) {
		List<Map<K, V>> maps = new ArrayList<Map<K, V>>();
		for (Map<K, V> map: delegates) {
			SortedMultimap<K, V> smap = (SortedMultimap<K, V>) map;
			maps.add(smap.tailMap(fromKey));
		}
		
		return new PDQSortedMultimap<K, V>(maps, comparator);
	}

	@Override
	public K firstKey() {
		SortedMap<K, V> smap = (SortedMap<K, V>) delegates.get(0);
		K firstKey = smap.firstKey();
		
		for (int i = partitions - 1; i > 0 ; i--) {
			smap = (SortedMap<K, V>) delegates.get(i);
			K key = smap.firstKey();
			if (compare(firstKey, key) > 0) {
				firstKey = key;
			}
		}
		
		return firstKey;
	}

	@Override
	public K lastKey() {
		SortedMap<K, V> smap = (SortedMap<K, V>) delegates.get(0);
		K lastKey = smap.lastKey();
		
		for (int i = partitions - 1; i > 0 ; i--) {
			smap = (SortedMap<K, V>) delegates.get(i);
			K key = smap.lastKey();
			if (compare(lastKey, key) < 0) {
				lastKey = key;
			}
		}
		
		return lastKey;
	}

	@Override
	public Object replicate() {
		return new PDQSortedMultimap<K, V>();
	}
}
