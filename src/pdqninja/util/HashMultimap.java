package pdqninja.util;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;


/**
 * Implementation of {@link Multimap} interface as a {@link HashMap}
 * of mappings between key and {@link ArrayList} of values.
 * <p>
 * Although the default implementation of this class is a {@link HashMap}
 * of keys to {@link ArrayList}, it is possible to change both:
 * <li>the mapping container type can be changed by subclassing and 
 * overriding the {@link #createContainer()} method.</li>
 * <li>the collection for storing values for a key can be changed 
 * by subclassing and overriding the {@link #createValuesCollection()}
 * method.</li>
 * <p>
 * As an example, a Multimap of {@link java.util.LinkedHashMap} as container, and
 * {@link java.util.HashSet} as values collection can be created as:
 * <pre>
 * {@code
 * class LinkedHashMapHashSetMultimap<K, V> extends HashMultimap<K, V> {
 *    protected Map<K, Collection<V>> createContainer() {
 *       return new LinkedHashMap<K, V>();
 *    }
 *    
 *    protected Collection<V> createValuesCollection() {
 *       return new HashSet<V>();
 *    }
 * }
 * } 
 * </pre>
 * @author mvarshney
 */
public class HashMultimap<K, V> implements Multimap<K, V>, Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 7331656429844467626L;
	final Map<K, Collection<V>> container;
	transient Collection<V> values;
	
	/**
	 * Constructs a new empty multimap.
	 */
	public HashMultimap() {
		this.container = createContainer();
	}

	/**
	 * Constructs a new map with the same mappings
	 * as the given map.
	 * 
	 * @param map the map whose mappings are to be 
	 * placed in this multimap
	 */
	public HashMultimap(Map<K, Collection<V>> map) {
		this.container = map;
	}
	
	/**
	 * Create the container object. The default implementation
	 * creates a {@link HashMap}. 
	 * @return the container object
	 */
	protected Map<K, Collection<V>> createContainer() {
		return new HashMap<K, Collection<V>>();
	}
	
	/**
	 * Creates the collection for storing values for a key.
	 * The default implementation creates an {@link ArrayList}.
	 * @return collection object for storing values for a key
	 */
	protected Collection<V> createValuesCollection() {
		return new ArrayList<V>();
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public int size() {
		return container.size();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean isEmpty() {
		return container.isEmpty();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean containsKey(Object key) {
		return container.containsKey(key);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean containsValue(Object value) {
		for (Collection<V> values : container.values()) {
			if (values.contains(value))
				return true;
		}
		return false;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public V get(Object key) {
		Collection<V> values = container.get(key);
		if (values == null) return null;
		if (values.size() == 0) return null;
		return values.iterator().next();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public V put(K key, V value) {
		Collection<V> values = container.get(key);
		if (values == null) {
			values = createValuesCollection();
		}
		values.add(value);
		container.put(key, values);
		return null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public V remove(Object key) {
		Collection<V> values = container.remove(key);
		if (values != null && values.size() > 0)
			return values.iterator().next();
		return null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void putAll(Map<? extends K, ? extends V> m) {
		if (HashMultimap.class.isAssignableFrom(m.getClass())) {
			@SuppressWarnings("unchecked")
			HashMultimap<K, V> map = (HashMultimap<K, V>) m;
			container.putAll(map.container);
		} else {
			for (Map.Entry<? extends K, ? extends V> entry : m.entrySet()) {
				put(entry.getKey(), entry.getValue());
			}
		}
		
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void clear() {
		container.clear();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Set<K> keySet() {
		return container.keySet();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Collection<V> values() {
		if (values == null) {
			values = new WrappedCollection<V>(container.values());
		}
		return values;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Set<java.util.Map.Entry<K, V>> entrySet() {
		return null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Collection<V> getAll(Object key) {
		return container.get(key);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean contains(K key, V value) {
		Collection<V> values = container.get(key);
		return (values == null) ?
				false : values.contains(value);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Set<java.util.Map.Entry<K, Collection<V>>> allEntrySet() {
		return container.entrySet();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean remove(Object key, Object value) {
		Collection<V> values = container.get(key);
		if (values == null)
			return false;
		return values.remove(value);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void put(K key, Collection<? extends V> values) {
		Collection<V> valueset = container.get(key);
		if (valueset == null) {
			valueset = createValuesCollection();
			container.put(key, valueset);
		}
		
		valueset.addAll(values);
	}
	
	@Override
	public String toString() {
		return container.toString();
	}

}
