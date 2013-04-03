package pdqninja.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Implementation of {@link SortedMultimap} interface as a {@link TreeMap}
 * of mappings between key and {@link ArrayList} of values.
 * <p>
 * Although the default implementation of this class is a {@link TreeMap}
 * of keys to {@link ArrayList}, it is possible to change both 
 * (see {@link HashMultimap}).  
 * 
 * @author mvarshney
 */
public class TreeMultimap<K, V> extends HashMultimap<K, V> implements SortedMultimap<K, V> {
	private static final long serialVersionUID = 9132373542627106143L;
	SortedMap<K, Collection<V>> scontainer;
	
	/**
	 * Constructs a new empty sortedmultimap.
	 */
	public TreeMultimap() {
		super();
		scontainer = (SortedMap<K, Collection<V>>) container;
	}
	
	/**
	 * Constructs a new map with the same mappings
	 * as the given map.
	 * 
	 * @param map the map whose mappings are to be 
	 * placed in this multimap
	 */
	public TreeMultimap(SortedMap<K, Collection<V>> map) {
		super(map);
		scontainer = map;
	}
	
	/**
	 * Create the container object. The default implementation
	 * creates a {@link TreeMap}. 
	 * @return the container object
	 */
	@Override
	protected Map<K, Collection<V>> createContainer() {
		return new TreeMap<K, Collection<V>>();
	}

	@Override
	public Comparator<? super K> comparator() {
		return scontainer.comparator();
	}

	@Override
	public SortedMultimap<K, V> subMap(K fromKey, K toKey) {
		return new TreeMultimap<K, V>(scontainer.subMap(fromKey, toKey));
	}

	@Override
	public SortedMultimap<K, V> headMap(K toKey) {
		return new TreeMultimap<K, V>(scontainer.headMap(toKey));
	}

	@Override
	public SortedMultimap<K, V> tailMap(K fromKey) {
		return new TreeMultimap<K, V>(scontainer.tailMap(fromKey));
	}

	@Override
	public K firstKey() {
		return scontainer.firstKey();
	}

	@Override
	public K lastKey() {
		return scontainer.lastKey();
	}
}
