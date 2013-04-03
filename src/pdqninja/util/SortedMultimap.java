package pdqninja.util;

import java.util.SortedMap;

/**
 * A {@link Multimap} that further provides a <em>total ordering</em>
 * on its keys.
 * 
 * This interface does not declare any new method beyond those 
 * inherited from the {@link Multimap} and the {@link SortedMap} interface.
 * The three methods ({@link #subMap(Object, Object)}, {@link #headMap(Object)}
 * and {@link #tailMap(Object)}) have been overridden in this interface to 
 * return object of <code>SortedMultimap</code> type.
 * 
 * @author mvarshney
 *
 * @param <K> the type of key
 * @param <V> they type of value
 */
public interface SortedMultimap<K, V> extends Multimap<K, V>, SortedMap<K, V> {
	/**
	 * Overridden method to return object of <code>SortedMultimap</code> type.
	 * {@inheritDoc}
	 */
	@Override
    SortedMultimap<K, V> subMap(K fromKey, K toKey);
	
	/**
	 * Overridden method to return object of <code>SortedMultimap</code> type.
	 * {@inheritDoc}
	 */
	@Override
    SortedMultimap<K,V> headMap(K toKey);
	
	/**
	 * Overridden method to return object of <code>SortedMultimap</code> type.
	 * {@inheritDoc}
	 */
	@Override
    SortedMultimap<K,V> tailMap(K fromKey);
}