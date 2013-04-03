package pdqninja.pdqcollections;

import java.io.Closeable;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
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
import pdqninja.pdq.PDQConfig;
import pdqninja.pdq.Splittable;

/**
 * A sorted map that stores the key-value mappings in a file.
 * This class provides append-only modifications to the underlying
 * data. Specifically, mappings added via the {@link #put(Object, Object)}
 * must be ordered on the keys. Methods such as {@link #remove(Object)} 
 * are not supported.
 * 
 * @author mvarshney
 */
class ExternalSortedMap<K, V> implements SortedMap<K, V>, Serializable, Splittable, Closeable {
	private static Logger logger = Logger.getLogger(ExternalSortedMap.class);
	
	private static final long serialVersionUID = 4558006799447540082L;

	final File file;
	final Comparator<? super K> comparator;
	long blocksize;
	
	MapFileIndex<K> index;
	transient MapFile<K> mapfile;
	
	K firstKey;
	K lastKey;
	int size;
	

	private transient KeySet keySet;
	private transient ValueSet valueSet;
	private transient EntrySet entrySet;
	
	/**
	 * Creates an ExternalSortedMap in read-write mode that uses
	 * the specified file for storing the key-value mappings. 
	 * The <tt>index</tt> specifies if an index must be created 
	 * for the map file.
	 * 
	 * @param file the file that is to be used for reading and
	 * writing the key-value mappings
	 * @param comparator the comparator for the keys
	 * @param indexed indicates if the map file should be indexed
	 * @throws IOException
	 */
	public ExternalSortedMap(
			File file, Comparator<? super K> comparator,
			boolean indexed) throws IOException {
		this.file = file;
		this.comparator = comparator;
		
		if (indexed)
			index = new MapFileIndex<K>(comparator);
		
		PDQConfig conf = PDQConfig.current();
		this.blocksize = conf.getBlocksize();
		
		this.mapfile = new MapFile<K>(file, "rw",
				index,
				conf.getExternalIndex(),
				conf.getReset(),
				(int) conf.getBuffer());

		if (logger.isDebugEnabled()) {
			logger.debug(String.format("created for %s " +
					"(mode=rw  indexed=%s)",
					file.getPath(), Boolean.toString(indexed)));
		}
	}
	
	/**
	 * Creates a read-only <em>view</em> of an existing ExternalSortedMap
	 * that begins with the <tt>firstKey</tt> and ends at the
	 * <tt>lastKey</tt>.
	 * 
	 * @param file the file that contains the key-value mappings
	 * @param mapfile the map file
	 * @param index the map file index
	 * @param firstKey the lower bound of the range
	 * @param firstKeyOffset the beginning file offset of the view 
	 * @param lastKey the upper bound of the range
	 * @param lastKeyOffset the ending file offset of the view
	 * @param size number of entries in the specified range
	 */
	public ExternalSortedMap(File file, MapFile<K> mapfile, MapFileIndex<K> index, 
			K firstKey, K lastKey, int size) {
		this.file = file;
		this.comparator = null;
		this.mapfile = mapfile;
		this.index = index;
		this.firstKey = firstKey;
		this.lastKey = lastKey;
		this.size = size;
		
		if (logger.isDebugEnabled()) {
			logger.debug(String.format("view created for %s " +
					"(firstKey=%s  lastKey=%s size=%d)",
					file.getPath(), firstKey.toString(),
					lastKey.toString(), size));
		}
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public int size() {
		return size;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean isEmpty() {
		return size == 0;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean containsKey(Object key) {
		return get(key) != null;
	}

	/**
	 * This method is not supported.
	 */
	@Override
	public boolean containsValue(Object value) {
		throw new UnsupportedOperationException();
	}

	/**
	 * {@inheritDoc}
	 */
	@SuppressWarnings("unchecked")
	@Override
	public V get(Object key) {
		if (mapfile.getIndex() == null)
			return null;
		
		MapFileIndex.Entry<K> entry = 
				mapfile.getIndex().getIndexForKey((K) key);
		
		if (entry == null) return null;
		
		try {
			mapfile.seek(entry.getOffset());
			while (true) {
				K k = (K) mapfile.read();
				V v = (V) mapfile.read();
				int cmp = compare((K) key, k);
		
				if (cmp == 0)
					return v;
				else if (cmp < 0)
					break;
			}
		} catch (IOException e) {
			
		} catch (ClassNotFoundException e) {
			
		}
		
		return null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public V put(K key, V value) {
		if (lastKey != null && compare(lastKey, key) >= 0)
			throw new IllegalArgumentException("key [" + key + "] must be greater than last key [" + lastKey + "]");
		
		put0(key, value);
		return null;
	}
	
	/**
	 * Write the key and the corresponding value into the
	 * map file. 
	 * @param key the key to write
	 * @param value the value to write
	 */
	void put0(K key, Object value) {
		try {
			mapfile.write(key, value);
			if (firstKey == null) firstKey = key;
			lastKey = key;
			size++;
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * This method is not supported.
	 */
	@Override
	public V remove(Object key) {
		throw new UnsupportedOperationException();
	}
	
	final class EntryComparator implements Comparator<Map.Entry<K, V>>, Serializable {
		private static final long serialVersionUID = 3132913531696509916L;

		@Override
		public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
			return ExternalSortedMap.this.compare(o1.getKey(), o2.getKey());
		}
	}
	
	final EntryComparator entryComparator = new EntryComparator();

	/**
	 * {@inheritDoc}
	 */
	//@SuppressWarnings("unchecked")
	@Override
	public void putAll(Map<? extends K, ? extends V> m) {
		if (m == null || m.size() == 0)
			return;
		
		if (m instanceof SortedMap) {
			SortedMap<? extends K, ? extends V> smap = (SortedMap<? extends K, ? extends V>) m;
			K key = smap.firstKey();
			if (lastKey != null && compare(lastKey, key) >= 0)
				throw new RuntimeException("key [" + key + "] must be greater than last key [" + lastKey + "]");
			
			for (Map.Entry<? extends K, ? extends V> entry: m.entrySet()) {
				put0(entry.getKey(), entry.getValue());
			}
			
		} else {
			long start = System.currentTimeMillis();
			List<Map.Entry<K, V>> list = 
					new ArrayList<Map.Entry<K, V>>(m.size());
			for (Map.Entry<? extends K, ? extends V> entry: m.entrySet()) {
				list.add((Map.Entry<K, V>) entry);
			}
			
			Collections.sort(list, entryComparator);
			
			long sortTime = System.currentTimeMillis();
			
			
			K key = list.get(0).getKey();
			if (lastKey != null && compare(lastKey, key) >= 0)
				throw new RuntimeException("key [" + key + "] must be greater than last key [" + lastKey + "]");
			
			for (Map.Entry<K, V> entry: list) {
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
		
	}

	/**
	 * This method is not supported
	 */
	@Override
	public void clear() {
		try {
			mapfile.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		file.delete();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Comparator<? super K> comparator() {
		return comparator;
	}
	
	long getOffsetForKey(K key) throws IOException {
		if (index == null) 
			throw new RuntimeException("MapIndex does not exist");
		
		 MapFileIndex.Entry<K> entry = index.getIndexForKey(key);
		 if (entry == null)
			 return 0;
		 
		long offset = entry.getOffset();
		mapfile.seek(offset);
		
		
		
		return 0;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public SortedMap<K, V> subMap(K fromKey, K toKey) {
		return subMap(fromKey, toKey, 0); // false
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public SortedMap<K, V> headMap(K toKey) {
		return subMap(firstKey, toKey, 0); // false
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public SortedMap<K, V> tailMap(K fromKey) {
		return subMap(fromKey, lastKey, 0); // true
	}
	
	/**
	 * Returns a submap view of this map.
	 *  
	 * @param fromKey the lower bound of the range
	 * @param toKey the upper bound of the range
	 * @param includeLastKey if the toKey must be included in the
	 * range
	 * @return a sub map view
	 */
	SortedMap<K, V> subMap(K fromKey, K toKey, int size) {
		if (compare(fromKey, toKey) > 0)
			return null;
		
		ExternalSortedMap<K, V> map = 
				createView(file, fromKey, toKey, size);
		return map;
	}
	
	ExternalSortedMap<K, V> createView(File file, K firstKey, K lastKey, int size) {
		return new ExternalSortedMap<K, V>(file, mapfile, index, 
				firstKey, lastKey, size);
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public K firstKey() {
		return firstKey;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public K lastKey() {
		return lastKey;
	}

	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public Set<K> keySet() {
		if (keySet == null)
			keySet = new KeySet();
		return keySet;
	}

	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public Collection<V> values() {
		if (valueSet == null)
			valueSet = new ValueSet();
		return valueSet;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Set<java.util.Map.Entry<K, V>> entrySet() {
		if (entrySet == null)
			entrySet = new EntrySet();
		return entrySet;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Iterator<?> getSplits() {
		return new SplitIterator();
	}

	@SuppressWarnings("unchecked")
	int compare(K first, K second) {
		if (comparator != null) {
			return comparator.compare(first, second);
		} else {
			return ((Comparable<K>) first).compareTo(second);
		}
	}

	
	public void flush() throws IOException {
		mapfile.flush();
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public void close() throws IOException {
		mapfile.close();
	}
	
	private void readObject(ObjectInputStream ois) throws IOException, ClassNotFoundException {
		ois.defaultReadObject();
		PDQConfig conf = PDQConfig.current();
		
		this.mapfile = new MapFile<K>(file, "r",
				index,
				conf.getExternalIndex(),
				conf.getReset(),
				(int) conf.getBuffer());
	}
	
	private void writeObject(ObjectOutputStream oos) throws IOException {
		oos.defaultWriteObject();
	}
	
	/**
	 * Represents one key-value mapping.
	 * 
	 * @author mvarshney
	 */
	static final class MapFileEntry<K, V> implements Map.Entry<K, V> {
		K key;
		V value;
		
		MapFileEntry(K key, V value) {
			this.key = key;
			this.value = value;
		}
		
		@Override
		public K getKey() {
			return key;
		}
		
		public void setKey(K key) {
			this.key = key;
		}

		@Override
		public V getValue() {
			return value;
		}

		@Override
		public V setValue(V value) {
			V old = value;
			this.value = value;
			return old;
		}
		
	}
	
	/**
	 * Iterates over the key-value mapping in the map file.
	 * 
	 * @author mvarshney
	 */
	abstract class MapFileIterator<E> implements Iterator<E> {
		Entry<K, V> next;
		MapFile<K> mapfile;
		int remaining;
		
		MapFileIterator() {
			remaining = size;
			
			try {
				mapfile = new MapFile<K>(file, "r");
				
				if (index != null) {
					long begin = index.getIndexForKey(firstKey).getOffset();
					mapfile.seek(begin);
				}
				
				nextEntry();
			} catch (IOException e) {
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
		
		final Entry<K, V> nextEntry() {
			Entry<K, V> e = next;
			next = null;
			
			if (remaining == 0) return e;
			remaining --;

			try {
				@SuppressWarnings("unchecked")
				K key = (K) mapfile.read();
				if (key == null) return e;

				@SuppressWarnings("unchecked")
				V value = (V) mapfile.read();

				next = new MapFileEntry<K, V>(key, value);


			} catch (EOFException ex) {

			} catch (IOException ex) {
				ex.printStackTrace();
			} catch (ClassNotFoundException ex) {
				ex.printStackTrace();
			}
			
			return e;
		}
	}

	
	private final class KeyIterator extends MapFileIterator<K> {

		@Override
		public K next() {
			return nextEntry().getKey();
		}
	}
	
	private final class ValueIterator extends MapFileIterator<V> {

		@Override
		public V next() {
			return nextEntry().getValue();
		}
	}
	
	
	private final class EntryIterator extends MapFileIterator<Map.Entry<K, V>> {

		@Override
		public Map.Entry<K, V> next() {
			return nextEntry();
		}
	}
	
	private final class KeySet extends AbstractSet<K> {
		
		@Override
		public Iterator<K> iterator() {
			return new KeyIterator();
		}

		@Override
		public int size() {
			return size;
		}
	}
	
	private final class ValueSet extends AbstractSet<V> {
		
		@Override
		public Iterator<V> iterator() {
			return new ValueIterator();
		}

		@Override
		public int size() {
			return size;
		}
	}
	
	private final class EntrySet extends AbstractSet<Map.Entry<K, V>> {
		
		@Override
		public Iterator<java.util.Map.Entry<K, V>> iterator() {
			return new EntryIterator();
		}

		@Override
		public int size() {
			return size;
		}		
	}
	
	private final class SplitIterator implements Iterator<SortedMap<K, V>> {
		private int fromIndex;
		private int toIndex = 0;
		private int numKeys = 0;
		private long blocksize;
		
		SplitIterator() {
			blocksize = PDQConfig.current().getBlocksize();
			updateNext();
		}

		private void updateNext() {
			MapFileIndex<K> index = mapfile.getIndex();
			if (index == null)
				return;
			
			fromIndex = toIndex;
			numKeys = 0;
			
			if (fromIndex == -1) return;
			
			int size = index.size();
			long begin = index.getIndex(fromIndex).getOffset();
			long end = -1;
			
			
			for (toIndex = fromIndex + 1; toIndex < size; toIndex ++) {
				MapFileIndex.Entry<K> indexEntry = index.getIndex(toIndex);
				end = indexEntry.getOffset();
				numKeys ++;
				numKeys += indexEntry.getSkipped();
				
				if ((end - begin) >= blocksize)
					break;
			}
			
			 if (toIndex >= (size - 1)) {
				toIndex = -1;
				numKeys ++;
				numKeys += index.skipped();
			}
						
			if (fromIndex == toIndex) numKeys = 0;
		}
		
		@Override
		public boolean hasNext() {
			return numKeys > 0;
		}

		@Override
		public SortedMap<K, V> next() {
			if (numKeys == 0) return null;
			
			MapFileIndex<K> index = mapfile.getIndex();
			boolean isLast = (toIndex == -1);
			
			K fromKey = index.getIndex(fromIndex).getKey();
			K toKey = isLast ? lastKey : index.getIndex(toIndex).getKey();

			if (logger.isDebugEnabled()) {
				logger.debug(String.format("[%s, %s] splitted to [%s, %s (numKeys=%d)]",
						firstKey.toString(),
						lastKey.toString(),
						fromKey, toKey, numKeys));
			}
			
			SortedMap<K, V> map = subMap(fromKey, toKey, numKeys);
			updateNext();
			return map;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}
		
	}
	
	@Override
	public String toString() {
		return String.format("ExternalMap %s (%d)", file.getPath(), file.length());
	}
}
