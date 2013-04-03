package pdqninja.pdqcollections;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;

import org.apache.log4j.Logger;

import pdqninja.io.MapFile;
import pdqninja.io.MapFileIndex;
import pdqninja.util.Multimap;
import pdqninja.util.SortedMultimap;

/**
 * A sorted multimap that stores the key-value mappings in a file.
 * This class provides append-only modifications to the underlying
 * data. Specifically, mappings added via the {@link #put(Object, Object)}
 * must be ordered on the keys. Methods such as {@link #remove(Object)} 
 * are not supported.
 * 
 * @author mvarshney
 */
class ExternalSortedMultimap<K, V> extends ExternalSortedMap<K, V> implements
		SortedMultimap<K, V> {
	private static Logger logger = Logger.getLogger(ExternalSortedMultimap.class);
	private static final long serialVersionUID = 163059669723719518L;
	transient private AllEntrySet allEntrySet; 
	
	
	public ExternalSortedMultimap(File file, Comparator<? super K> comparator,
			boolean indexed) throws IOException {
		super(file, comparator, indexed);
	}

	public ExternalSortedMultimap(File file, MapFile<K> mapfile, 
			MapFileIndex<K> index,
			K firstKey, K lastKey, int size) {
		super(file, mapfile, index, firstKey, lastKey, size);
	}
	
	@Override
	public V put(K key, V value) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void put(K key, Collection<? extends V> values) {
		if (lastKey != null && compare(lastKey, key) >= 0)
			throw new IllegalArgumentException("key [" + key + "] must be greater than last key [" + lastKey + "]");
		
		put0(key, values);
	}

	@Override
	public boolean contains(K key, V value) {
		Collection<V> collection = getAll(key);
		return collection.contains(value);
	}

	@Override
	public boolean remove(Object key, Object value) {
		throw new UnsupportedOperationException();
	}
	
	final class AllEntryComparator implements Comparator<Map.Entry<K, Collection<V>>>, Serializable {
		private static final long serialVersionUID = 8487141589886171354L;

		@Override
		public int compare(Map.Entry<K, Collection<V>> o1, Map.Entry<K, Collection<V>> o2) {
			return ExternalSortedMultimap.this.compare(o1.getKey(), o2.getKey());
		}
	}
	
	final AllEntryComparator allEntryComparator = new AllEntryComparator();
	
	@Override
	public void putAll(Map<? extends K, ? extends V> m) {
		if (m == null || m.size() == 0)
			return;
		
		boolean isSorted = (m instanceof SortedMap);
		boolean isMulti = (m instanceof Multimap);
		
		if (isMulti) {
			Multimap<? extends K, ? extends V> mmap = (Multimap<? extends K, ? extends V>) m;
			if (isSorted) {
				SortedMultimap<? extends K, ? extends V> smmap = (SortedMultimap<? extends K, ? extends V>) mmap;
				K key = smmap.firstKey();
				if (lastKey != null && compare(lastKey, key) >= 0)
					throw new RuntimeException("key [" + key + "] must be greater than last key [" + lastKey + "]");
				
				for (Map.Entry<? extends K, ?> entry: mmap.allEntrySet()) {
					put0(entry.getKey(), entry.getValue());
				}
			} else {
				long start = System.currentTimeMillis();
				List<Map.Entry<K, Collection<V>>> list = 
						new ArrayList<Map.Entry<K, Collection<V>>>(m.size());
				
				Iterator<? extends Map.Entry<? extends K, ? extends Collection<? extends V>>> it =
						mmap.allEntrySet().iterator();
				
				while (it.hasNext()) {
					list.add((Map.Entry<K, Collection<V>>) it.next());
				}
				
				Collections.sort(list, allEntryComparator);
				
				long sortTime = System.currentTimeMillis();
				
				K key = list.get(0).getKey();
				if (lastKey != null && compare(lastKey, key) >= 0)
					throw new RuntimeException("key [" + key + "] must be greater than last key [" + lastKey + "]");
				
				for (Map.Entry<K, Collection<V>> entry: list) {
					put0(entry.getKey(), entry.getValue());
				}
				
				if (logger.isInfoEnabled()) {
					long duration = System.currentTimeMillis() - sortTime;
					long bytes = this.file.length();

					logger.info(String.format("\tSorted %d keys in %dms", 
							list.size(),
							sortTime - start));

					logger.info(String.format("\tExternalized %.2fMB in %dsec. Rate=%.2f", 
							1.0 * bytes / 1024 / 1024,
							duration,
							1.0 * bytes / 1024 / 1024 / duration * 1000));
				}
			}
		} else {
			if (isSorted) {
				
			} else {
				
			}
		}
		
	}
	
	@Override 
	public Set<Map.Entry<K, V>> entrySet() {
		throw new UnsupportedOperationException();
	}

	@Override
	public Set<Map.Entry<K, Collection<V>>> allEntrySet() {
		if (allEntrySet == null)
			allEntrySet = new AllEntrySet();
		return allEntrySet;
	}

	@Override
	public V get(Object key) {
		Collection<V> collection = getAll(key);
		Iterator<V> it = collection.iterator();
		return it.hasNext() ? it.next() : null;
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public Collection<V> getAll(Object key) {
		return (Collection<V>) super.get(key);
	}

	
	@Override
	public SortedMultimap<K, V> headMap(K toKey) {
		return (SortedMultimap<K, V>) subMap(firstKey, toKey, 0);
	}
	
	@Override
	public SortedMultimap<K, V> tailMap(K fromKey) {
		return (SortedMultimap<K, V>) subMap(fromKey, lastKey, 0);
	}
	
	@Override
	public SortedMultimap<K, V> subMap(K fromKey, K toKey) {
		return (SortedMultimap<K, V>) subMap(fromKey, toKey, 0);
	}
	
	
	@Override
	ExternalSortedMultimap<K, V> createView(File file, 
			K firstKey, K lastKey, int size) {
		return new ExternalSortedMultimap<K, V>(file, mapfile, index,
				firstKey, lastKey, size);
	}
	
	/**
	 * 
	 * @author mvarshney
	 *
	 */
	class AllEntryIterator implements Iterator<Map.Entry<K, Collection<V>>> {
		Entry<K, Collection<V>> next;
		MapFile<K> mapfile;
		int remaining;
		
		AllEntryIterator() {
			remaining = size;
			
			try {
				mapfile = new MapFile<K>(file, "r");
				
				if (index != null) {
					long begin = index.getIndexForKey(firstKey).getOffset();
					mapfile.seek(begin);
				}
				
				
				nextEntry();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
			
		@Override
		public final boolean hasNext() {
			return next != null;
		}
		
		@Override
		public final void remove() {
			throw new UnsupportedOperationException();
		}
		
		final Entry<K, Collection<V>> nextEntry() {
			Entry<K, Collection<V>> e = next;
			next = null;
			
			if (remaining == 0) return e;
			remaining --;
			
			try {
				@SuppressWarnings("unchecked")
				K key = (K) mapfile.read();
				if (key == null) return e;
				
				@SuppressWarnings("unchecked")
				Collection<V> value = (Collection<V>) mapfile.read();

				next = new MapFileEntry<K, Collection<V>>(key, value);
			} catch (EOFException ex) {
				
			} catch (IOException ex) {
				ex.printStackTrace();
			} catch (ClassNotFoundException ex) {
				ex.printStackTrace();
			}
			
			return e;
		}

		@Override
		public java.util.Map.Entry<K, Collection<V>> next() {
			return nextEntry();
		}
	}
	
	private final class AllEntrySet extends AbstractSet<Map.Entry<K, Collection<V>>> {
		
		@Override
		public Iterator<java.util.Map.Entry<K, Collection<V>>> iterator() {
			return new AllEntryIterator();
		}

		@Override
		public int size() {
			return size;
		}		
	}

}
