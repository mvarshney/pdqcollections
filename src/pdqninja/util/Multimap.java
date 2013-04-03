package pdqninja.util;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * An object that maps keys to a collection of values.
 * <p>
 * As opposed to {@link java.util.Map} that maps a key 
 * uniquely to a value, a <code>Multimap</code> can map a 
 * key to more than one values. The implementing class choose
 * if duplicate values are allowed.
 * for a key.
 * <p>
 * This interface extends the <code>java.util.Map</code> interface
 * with methods that operate on a collection of values, as outlined
 * in the following:
 * <p>
 * Whereas {@link Map#put(Object, Object)} adds one key/value pair
 * in the Multimap, the {@link #put(Object, Collection)} method can
 * add multiple values for a key in a single method call.
 * <p>
 * {@link #contains(Object, Object)} checks if a given value for a
 * given key (amongst is potentially multiple values) exists.
 * Similarly, {@link #remove(Object, Object)} removes one value
 * from amongst the potentially multiple values mapped to a key.
 * <p>
 * {@link #allEntrySet()} returns a set view of mappings between
 * key and a collection of values. {@link #getAll(Object)} returns
 * all values mapped to a key.
 * <p>
 * The implementation may choose how they handle the {@link #entrySet()}
 * and {@link #get(Object)} methods. They may return the "first" value
 * (if some ordering exists), a non-deterministically selected value, or
 * throw an <code>UnsupportedOperationException</code> exception. 
 *   
 * @author mvarshney
 *
 * @param <K> the type of key
 * @param <V> the type of value
 */
public interface Multimap<K, V>  extends Map<K, V> {
	/**
	 * Associate the specified collection of values with the
	 * specified key in this Multimap. If the key previously existed,
	 * the new collection of values will be "added" to the previous
	 * collection of values. 
	 * <p>
	 * Note that the implementing class is responsible for deciding
	 * if duplicate values are allowed for a key.
	 * 
	 * @param key key with which specified collection of values is associated.
	 * @param values collection of values to associate with the key.
	 */
	void put(K key, Collection<? extends V> values);
	
	/**
	 * Returns <code>true</code> if the Multimap maps the
	 * specified key with the specified value.
	 * 
	 * @param key the key in the (key, value) pair whose presence
	 * in this Multimap is to be tested.
	 * @param value the value in the (key, value) pair whose presense
	 * in this Multimap is to be tested.
	 * @return <code>true</code> if the specified (key, value) exists
	 * in the Multimap.
	 */
	boolean contains(K key, V value);
	
	/**
	 * Removes the specified value for the specified key from this
	 * Multimap.
	 * 
	 * @param key key the key in the (key, value) pair that is to
	 * be removed
	 * @param value value the key in the (key, value) pair that is to
	 * be removed
	 * @return <code>true</code> if the pair was removed
	 */
	boolean remove(Object key, Object value);
	
	/**
	 * Returns a {@link Set} view of the mappings in this Multimap.
	 * Each item in the set is a mapping between a key and a collection
	 * of values.
	 * 
	 * @return a set view of the mappings in this Multimap.
	 */
	Set<Map.Entry<K, Collection<V>>> allEntrySet();
	
	/**
	 * Returns a collection of values associated to the specified key, or 
	 * <code>null</code> if no mapping exists for the key.
	 * 
	 * @param key key whose associated collection of values is
	 * to be returned.
	 * @return a collection of values associated to the specified key,
	 * or <code>null</code> if no mapping exists for the key.
	 */
	Collection<V> getAll(Object key);
}
