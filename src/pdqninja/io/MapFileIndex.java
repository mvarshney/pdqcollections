package pdqninja.io;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Maintains an index for the MapFiles. {@link MapFile} stores 
 * a sequence of keys
 * (and optionally associated values) in a file. An index can
 * be created for this file, where each entry in the index will be
 * a mapping between the key and the offset within the file
 * where that key is written. The size of the index can be reduced
 * if entries are created only for a subset of the keys. For example, the 
 * <tt>MapFile</tt> creates one index entry for every
 * <tt>bytesPerIndex</tt> number of bytes.
 * <p>
 * This class manages the index for the {@linkplain MapFile}.
 * The index is created when the keys are written to the map file. For
 * each key written to the file, this index object is notified: either
 * via the {@link #createIndex(Object, long)} to create an index entry, 
 * or the {@link #skip()} method to indicate that a key is being 
 * written to the file, but no index entry must be created (that is,
 * we are skipping this key in the index).
 * <p>
 * The {@link #getIndex(int)} returns the <em>i-th</em> index entry.
 * If it is known that the keys are stored in sorted order in the
 * map file, this index can be used to efficiently
 * retrieve keys and their values. The {@link #getIndexForKey(Object)}
 * can be used to locate the neighborhood of a given key. This method
 * returns the exact file offset if this key was part of the index,
 * or the file offset of the largest key that is smaller than this
 * key.
 * <p>
 * This index can be written to a file using the {@link #write(File)}
 * method, and read from a file using the {@link #read(File)} method.
 * In both cases, it is assumed that the keys are serializable.
 *  
 * @author mvarshney
 */
public class MapFileIndex<K> implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 6021484007229992088L;

	/**
	 * A single entry in the index. The entry consists of the key,
	 * the offset within the file where this key is written,
	 * and the number of keys that were skipped in the index since
	 * the last entry in the index.
	 * 
	 *  
	 * @author mvarshney
	 */
	public static final class Entry<K> implements Serializable {
		/**
		 * 
		 */
		private static final long serialVersionUID = 157422485426758481L;
		K key;
		long offset;
		int skipped;
		
		public Entry(K key, long offset, int skipped) {
			this.key = key;
			this.offset = offset;
			this.skipped = skipped;
		}

		
		/**
		 * @return the key
		 */
		public K getKey() {
			return key;
		}

		/**
		 * @return the offset
		 */
		public long getOffset() {
			return offset;
		}

		/**
		 * @return the skipped
		 */
		public int getSkipped() {
			return skipped;
		}


		/* (non-Javadoc)
		 * @see java.lang.Object#toString()
		 */
		@Override
		public String toString() {
			return "Entry [key=" + key + ", offset=" + offset + ", skipped="
					+ skipped + "]";
		}
		
	}
	
	/**
	 * Comparator for the Entry objects.
	 * 
	 * @author mvarshney
	 */
	private static final class EntryComparator<K> implements Comparator<Entry<K>>, Serializable {
		
		/**
		 * 
		 */
		private static final long serialVersionUID = 7325362283290552526L;
		final Comparator<? super K> comparator;
		
		EntryComparator(Comparator<? super K> comparator2) {
			this.comparator = comparator2;
		}
		
		@SuppressWarnings("unchecked")
		@Override
		public int compare(Entry<K> o1, Entry<K> o2) {
			if (comparator != null) {
				return comparator.compare(o1.key, o2.key);
			} else {
				return ((Comparable<K>) o1.key).compareTo(o2.key);
			}
		}
		
	}
	
	private final List<Entry<K>> entries;
	private final EntryComparator<K> entryComparator;
	private int skipped = 0;
	
	/**
	 * Creates a MapFileIndex for keys that implement the
	 * Comparable interface.
	 */
	public MapFileIndex() {
		this(null);
	}
	
	/**
	 * Creates a MapFileIndex for keys by using the specified 
	 * comparator.
	 * @param comparator the comparator for keys
	 */
	public MapFileIndex(Comparator<? super K> comparator) {
		entryComparator = new EntryComparator<K>(comparator);
		entries = new ArrayList<Entry<K>>();
	}
	
	/**
	 * Creates a MapFileIndex that is subset view of other index.
	 * 
	 * @param entries the list of entries in this index
	 * @param entryComparator the comparator for the entries
	 */
	public MapFileIndex(List<Entry<K>> entries, EntryComparator<K> entryComparator) {
		this.entryComparator = entryComparator;
		this.entries = entries;
	}
	
	/**
	 * Indicates that a key has been written to the map file,
	 * but it should not be added to the index.
	 */
	public void skip() {
		skipped ++;
	}
	
	/**
	 * Indicates that a key has been written to the map file,
	 * and an entry must be created for this key.
	 * @param key the key for which the index entry is to be created
	 * @param offset the file offset where this key is written
	 */
	public void createIndex(K key, long offset) {
		entries.add(new Entry<K>(key, offset, skipped));
		skipped = 0;
	}
	
	/**
	 * Returns the entry at the specified position in the index.
	 * 
	 * @param index the position in the index
	 * @return index entry at the specified position
	 */
	public Entry<K> getIndex(int index) {
		return entries.get(index);
	}
	
	/**
	 * Return the largest index entry that is smaller
	 * than the specified key. Returns <tt>null</tt> if
	 * no such index entry exists.
	 *  
	 * @param key the specified key
	 * @return largest index entry that is smaller than
	 * the specified key
	 */
	public Entry<K> getIndexForKey(K key) {
		int idx = getIndexForKey(key, true);
		return idx < 0 ? null : entries.get(idx);
	}
	
	/**
	 * Returns an index entry closest to the specified key.
	 * If <tt>lowerBound</tt> is true, return the largest 
	 * index entry that is smaller than the specified key,
	 * otherwise, return the smallest index entry that is 
	 * greater than the specified key.
	 * 
	 * @param key the specified key
	 * @param lowerBound indicates if the index entry must
	 * be smaller than or greater than the specified key
	 * @return an index entry that is closest to the specified key,
	 * or <tt>null</tt> if no such entry exists
	 */
	int getIndexForKey(K key, boolean lowerBound) {
		int size = size();
		int low = 0;
		int high = size - 1;
		Entry<K> entry = new Entry<K>(key, 0, 0);
		
		while (low <= high) {
			int mid = (low + high) >>> 1;
			Entry<K> midEntry = entries.get(mid);
			int cmp = entryComparator.compare(entry, midEntry);			
			
			if (cmp == 0)
				return mid;
			else if (cmp > 0) 
				low = mid + 1;
			else
				high = mid - 1;
		}
		
		if (lowerBound) {
			low--;
		}
		
		if (low >= 0 && low < size)
			return low;
		
		return -1;
	}
	
	/**
	 * Returns the number of entries in the index.
	 * 
	 * @return the number of entries in the index
	 */
	public int size() {
		return entries.size();
	}
	
	/**
	 * Returns the number of keys skipped since the last 
	 * index entry created.
	 * 
	 * @return number of keys skipped since the last 
	 * index entry created
	 */
	public int skipped() {
		return skipped;
	}
	
	/**
	 * Creates a <em>view</em> of the index that includes
	 * all keys ranging from <tt>fromKey</tt> and <tt>toKey</tt>.
	 * This sub index begins with the largest index entry that
	 * is smaller than the fromKey and ends with smallest index entry
	 * that is greater than the toKey.
	 *  
	 * @param fromKey the lower bound of the sub index
	 * @param toKey the upper bound of the sub index
	 * @return a sub index view that contains keys in the specified
	 * range
	 */
	public MapFileIndex<K> subIndex(K fromKey, K toKey) {
		int[] result = locateSubIndex(fromKey, toKey);
		if (result[0] == -1 || result[1] == -1)
			return new MapFileIndex<K>(new ArrayList<Entry<K>>(),
					entryComparator);
		
		return new MapFileIndex<K>(
				entries.subList(result[0], result[1] + 1), entryComparator);
	}
	
	/**
	 * Returns an approximate number of keys between the specified 
	 * key range. The number of keys is computed between two
	 * index entries: the lower bound is the largest index entry that
	 * is smaller than the fromKey, and the upper bound is the
	 * smallest index entry that is greater than the toKey.
	 * 
	 * @param fromKey the lower bound of the range
	 * @param toKey the upper bound of the range
	 * @return the approximate number of keys in the range
	 */
	public int distance(K fromKey, K toKey) {
		int[] result = locateSubIndex(fromKey, toKey);
		int low = result[0];
		int high = result[1];
		
		if (low == -1 || high == -1)
			return 0;
		
		int distance = 0;
		
		for (int i = low + 1; i <= high; i++)
			distance += entries.get(i).getSkipped() + 1;
		
		return distance;
	}
	
	/**
	 * Locates the index entries that bound the specified range.
	 * 
	 * @param fromKey the lower bound of the range
	 * @param toKey the upper bound of the range
	 * @return a two-element array, where the first element is the
	 * index of the entry smaller than the fromKey, and the second 
	 * element is the index of the entry that is greater than the
	 * toKey
	 */
	int[] locateSubIndex(K fromKey, K toKey) {
		int[] result = new int[2];
		result[0] = result[1] = -1;
		
		Entry<K> fromEntry = new Entry<K>(fromKey, 0, 0);
		Entry<K> toEntry = new Entry<K>(toKey, 0, 0);
		
		if (entryComparator.compare(fromEntry, toEntry) > 0)
			throw new IllegalArgumentException(
					String.format("fromKey [%s] is greater than toKey [%s]",
							fromKey, toKey));
		
		int size = size();
		
		if ((size == 0) 
				|| (entryComparator.compare(fromEntry, entries.get(size - 1)) > 0)
				|| (entryComparator.compare(toEntry, entries.get(0)) < 0)) {
			return result;
		}
		
		int low;
		int high;
		
		if (entryComparator.compare(fromEntry, entries.get(0)) < 0) {
			low = 0;
		} else {
			low = getIndexForKey(fromKey, true);
		}
		
		if (entryComparator.compare(toEntry, entries.get(size - 1)) > 0) {
			high = size - 1;
		} else {
			high = getIndexForKey(toKey, false);
		}
		
		result[0] = low;
		result[1] = high;
		
		return result;
		
	}
	
	/**
	 * Writes the index to a file.
	 * 
	 * @param file the file to which the index will be written
	 * @throws FileNotFoundException
	 * @throws IOException
	 * @see read
	 */
	public void write(File file) throws FileNotFoundException, IOException {
		ObjectOutputStream oos = 
				new ObjectOutputStream(
						new BufferedOutputStream(
								new FileOutputStream(file)));
		
		for (Entry<K> entry: entries) {
			oos.writeObject(entry);
		}
		
		oos.close();
	}
	
	/**
	 * Reads the index from a file.
	 * 
	 * @param file the file from which the index is to be read
	 * @throws FileNotFoundException
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @see write
	 */
	public void read(File file) throws FileNotFoundException, IOException, ClassNotFoundException {
		ObjectInputStream ois =
				new ObjectInputStream(
						new BufferedInputStream(
								new FileInputStream(file)));
		
		
		while (true) {
			try {
				@SuppressWarnings("unchecked")
				Entry<K> entry = (Entry<K>) ois.readObject();
				entries.add(entry);
			} catch (EOFException e) {
				break;
			}
		}
		
		ois.close();
	}

}
