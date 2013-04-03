package pdqninja.pdqcollections;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import pdqninja.pdq.Adder;


public class PDQSortedMap<K, V> extends PDQMap<K, V> implements SortedMap<K, V> {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -6799250538534907617L;

	public PDQSortedMap() {
		super();
	}

	public PDQSortedMap(Adder<V> adder) {
		super(adder);
	}

	public PDQSortedMap(Comparator<? super K> comparator, Adder<V> adder) {
		super(comparator, adder);
	}

	public PDQSortedMap(Comparator<? super K> comparator) {
		super(comparator);
	}

	public PDQSortedMap(List<Map<K, V>> maps,
			Comparator<? super K> comparator, Adder<V> adder) {
		super(maps, comparator, adder);
	}

	@Override
	protected boolean isSorted() {
		return true;
	}
	
	@Override
	protected Map<K, V> createDelegate() {
		return new TreeMap<K, V>();
	}
	
	@Override
	public Comparator<? super K> comparator() {
		return comparator;
	}


	@Override
	public SortedMap<K, V> subMap(K fromKey, K toKey) {
		List<Map<K, V>> maps = new ArrayList<Map<K, V>>();
		for (Map<K, V> map: delegates) {
			SortedMap<K, V> smap = (SortedMap<K, V>) map;
			maps.add(smap.subMap(fromKey, toKey));
		}
		
		return new PDQSortedMap<K, V>(maps, comparator, adder);
	}

	@Override
	public SortedMap<K, V> headMap(K toKey) {
		List<Map<K, V>> maps = new ArrayList<Map<K, V>>();
		for (Map<K, V> map: delegates) {
			SortedMap<K, V> smap = (SortedMap<K, V>) map;
			maps.add(smap.headMap(toKey));
		}
		
		return new PDQSortedMap<K, V>(maps, comparator, adder);
	}

	@Override
	public SortedMap<K, V> tailMap(K fromKey) {
		List<Map<K, V>> maps = new ArrayList<Map<K, V>>();
		for (Map<K, V> map: delegates) {
			SortedMap<K, V> smap = (SortedMap<K, V>) map;
			maps.add(smap.tailMap(fromKey));
		}
		
		return new PDQSortedMap<K, V>(maps, comparator, adder);
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
		return new PDQSortedMap<K, V>();
	}
}
