package pdqninja.pdqcollections;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import pdqninja.pdq.Adder;
import pdqninja.pdq.Mergeable;
import pdqninja.pdq.PDQ;
import pdqninja.pdq.PDQConfig;
import pdqninja.pdq.Splittable;
import pdqninja.pdqio.FileStoreIterator;
import pdqninja.pdqio.FileStores;
import pdqninja.util.FireOnceIterator;
import pdqninja.util.Multimap;
import pdqninja.util.SortedMultimap;
import pdqninja.util.WrappedCollection;
import pdqninja.util.WrappedIterator;
import pdqninja.util.WrappedSortedIterator;

/**
 *  
 * @author mvarshney
 *
 */
public class PDQMap<K, V> implements Map<K, V>, Splittable, Mergeable, Serializable {
	static Logger logger = Logger.getLogger(PDQMap.class);
	
	private static final long serialVersionUID = -7015088132189967902L;
	final List<Map<K, V>> delegates = new ArrayList<Map<K, V>>();
	final int partitions;
	
	Comparator<? super K> comparator;
	Adder<V> adder;
	transient Set<K> keySet;
	transient Collection<V> values;
	transient Set<Map.Entry<K, V>> entrySet;
	transient FileStoreIterator tmpStorage = null; 
	transient FileStoreIterator sharedStorage = null; 
	
	/**
	 * 
	 */
	public PDQMap() {
		PDQConfig config = PDQConfig.current();
		
		this.partitions = config.isDistributable() ?
				config.getNumWorkers() * config.getThreads() :
				config.getThreads();
		
		
		for (int i = 0; i < partitions; i++) {
			this.delegates.add(createDelegate());
		}
	}
	
	public PDQMap(Comparator<? super K> comparator) {
		this();
		this.comparator = comparator;
	}
	
	public PDQMap(Adder<V> adder) {
		this();
		this.adder = adder;
	}
	
	public PDQMap(Comparator<? super K> comparator, 
			Adder<V> adder) {
		this();
		this.comparator = comparator;
		this.adder = adder;
	}
	
	public PDQMap(Collection<? extends Map<K, V>> maps,
			Comparator<? super K> comparator, 
			Adder<V> adder) {
		this.partitions = maps.size();
		this.delegates.addAll(maps);
		
		this.comparator = comparator;
		this.adder = adder;	
	}
	
	/**
	 * 
	 * @return
	 */
	protected Map<K, V> createDelegate() {
		return new HashMap<K, V>();
	}
	
	protected boolean isSorted() {
		return false;
	}
	
	protected boolean isMultimap() {
		return false;
	}
	
	File newExternalMapFile(boolean isIntermediate) throws IOException {
		if (!isIntermediate) {
			if (sharedStorage == null) {
				try {
					sharedStorage = FileStores.getSharedStorage();
				} catch (IOException e) {
					
				}
			}
			
			if (sharedStorage != null) {
				sharedStorage.next();
				return sharedStorage.createFile("final");
			}
		}
		
		if (tmpStorage == null) {
			try {
				tmpStorage = FileStores.getLocalStorage();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		tmpStorage.next();
		
		return tmpStorage.createFile(isIntermediate ? "tmp" : "final");
	}
	
	/**
	 * 
	 * @param isIntermediate
	 * @return
	 * @throws IOException
	 */
	ExternalSortedMap<K, V> newExternalMap(boolean isIntermediate) throws IOException {
		return new ExternalSortedMap<K, V>(
				newExternalMapFile(isIntermediate),
				comparator,
				!isIntermediate);
	}
	
	Map<K, V> map(int hashcode) {
		return delegates.get(Math.abs(hashcode) % partitions);
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public int size() {
		int size = 0;
		for (Map<K, V> map : delegates) {
			size += map.size();
		}
		return size;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean isEmpty() {
		for (Map<K, V> map : delegates) {
			if (!map.isEmpty()) return false;
		}
		return true;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean containsKey(Object key) {
		return map(key.hashCode()).containsKey(key);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean containsValue(Object value) {
		for (Map<K, V> map : delegates) {
			if (map.containsValue(value)) return true;
		}
		return false;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public V get(Object key) {
		return map(key.hashCode()).get(key);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public V put(K key, V value) {
		return map(key.hashCode()).put(key, value);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public V remove(Object key) {
		return map(key.hashCode()).remove(key);
	}

	@Override
	public void putAll(Map<? extends K, ? extends V> m) {
		if (PDQMap.class.isAssignableFrom(m.getClass())) {
			PDQMap<? extends K, ? extends V> pmap = 
					(PDQMap<? extends K, ? extends V>) m;
			for (int i = 0; i < partitions; i ++) {
				// TODO: in parallel
				delegates.get(i).putAll(pmap.delegates.get(i));
			}
		} else {
			for (Map.Entry<? extends K, ?extends V> entry : m.entrySet()) {
				put(entry.getKey(), entry.getValue());
			}
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void clear() {
		for (Map<K, V> map : delegates) {
			map.clear();
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Set<K> keySet() {
		
		if (keySet == null) {
			List<Set<K>> keySets = new ArrayList<Set<K>>();
			for (Map<K, V> map : delegates) {
				keySets.add(map.keySet());
			}
			if (isSorted()) {
				keySet = new PDQSet<K>(keySets, comparator) {
					/**
					 * 
					 */
					private static final long serialVersionUID = -9037943627752121501L;

					@Override
					public Iterator<K> iterator() {
						List<Iterator<K>> iterators = new ArrayList<Iterator<K>>();
						for (Collection<K> collection : delegates) {
							iterators.add(collection.iterator());
						}
						return new WrappedSortedIterator<K>(iterators);
					}
				};
			} else {
				keySet = new PDQSet<K>(keySets, comparator);
			}
		}
		
		return keySet;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Collection<V> values() {
		if (values == null) {
			List<Collection<V>> allvalues = 
					new ArrayList<Collection<V>>(partitions);
			for (Map<K, V> map : delegates) {
				allvalues.add(map.values());
			}
			values = new WrappedCollection<V>(allvalues);
		}
		return values;
	}

	final class EntryComparator implements Comparator<Map.Entry<K, V>>, Serializable {
		/**
		 * 
		 */
		private static final long serialVersionUID = -5560681768887059147L;

		@Override
		public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
			return PDQMap.this.compare(o1.getKey(), o2.getKey());
		}
	}
	
	final EntryComparator entryComparator = new EntryComparator();
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public Set<java.util.Map.Entry<K, V>> entrySet() {
		
		if (entrySet == null) {
			List<Set<Map.Entry<K, V>>> entrySets = 
					new ArrayList<Set<Map.Entry<K, V>>>(partitions);
			for (Map<K, V> map : delegates) {
				entrySets.add(map.entrySet());
			}
			
			if (isSorted()) {
				entrySet = new PDQSet<Map.Entry<K, V>>(entrySets, entryComparator) {
					/**
					 * 
					 */
					private static final long serialVersionUID = 4083477075976311691L;

					@Override
					public Iterator<Map.Entry<K, V>> iterator() {
						List<Iterator<Map.Entry<K, V>>> iterators = 
								new ArrayList<Iterator<Map.Entry<K, V>>>();
						for (Collection<Map.Entry<K, V>> collection : delegates) {
							iterators.add(collection.iterator());
						}
						return new WrappedSortedIterator<Map.Entry<K, V>>(iterators, comparator);
					}
				};
			} else {
				entrySet = new PDQSet<Map.Entry<K, V>>(entrySets, entryComparator);
			}
		}
		
		return entrySet;
	}

	@Override
	public Object replicate() {
		return new PDQMap<K, V>();
	}

	@Override
	public void merge(Object... objects) throws IOException, InterruptedException {
		if (objects != null) {
			int nobjects = objects.length;
			Object[][] args = new Object[partitions][nobjects];
			
			/* It is assumed that each 'object' is a PDQMap (or derived)
			 * object.
			 * Shuffle the objects to be merged into 2D array as:
			 * 	index 0: delegate_0_of_object_0, delegate_0_of_object_1, ...
			 * 	index 1: delegate_1_of_object_0, delegate_1_of_object_1, ...
			 * 	index t: delegate_t_of_object_0, delegate_t_of_object_1, ...
			 * t = N - 1, where
			 * N = number of partitions (i.e. number of delegates) 
			 */
			for (int i = 0; i < nobjects; i++) {
				if (objects[i] == null) continue;

				@SuppressWarnings("unchecked")
				PDQMap<K, V> asMap = (PDQMap<K, V>) objects[i];
				
				for (int j = 0; j < partitions; j++) {
					args[j][i] = asMap.delegates.get(j);
				}
			}
			
			
			List<Callable<Long>> tasks = new ArrayList<Callable<Long>>();
			
			for (int i = 0; i < partitions; i ++) {
				Map<K, V> delegate = delegates.get(i);
				
				if (! isDelegatePartial(i)) {
					delegates.set(i, new PartialMap(delegate));
					delegate = delegates.get(i);
				}
				PartialMap partialMap = (PartialMap) delegate;
				Callable<Long> task = partialMap.createMergeTask(args[i]);
				if (task != null) tasks.add(task);
			}
			runTasks("merging intermediate", tasks);
			
			args = null;
			
			if (PDQConfig.current().isGarbageCollect())
				System.gc();
			
			// Decide if the delegates must be externalized
			long threshold = PDQConfig.current().getMinFree();
			long freeMemory = Runtime.getRuntime().freeMemory();
					
			if (freeMemory < threshold) {
				if (logger.isInfoEnabled()) {
					logger.info(String.format("Will externalize. Free=%.2fMB Threshold=%.2fMB",
							1.0 * freeMemory / 1024 / 1024, 
							1.0 * threshold / 1024 / 1024));
				}
				
				for (int i = 0; i < partitions; i ++) {
					PartialMap partialMap = (PartialMap) delegates.get(i);
					Callable<Long> task = partialMap.externalize(true);
					if (task != null) tasks.add(task);
				}
				
				runTasks("Externalizing intermediate", tasks);	
			}
			
		} else {
			List<Callable<Long>> tasks = new ArrayList<Callable<Long>>();
			
			for (int i = 0; i < partitions; i ++) {
				if (! isDelegatePartial(i)) continue;
				PartialMap partialMap = (PartialMap) delegates.get(i);
				Callable<Long> task = partialMap.externalize(false);
				if (task != null) tasks.add(task);
			}
			
			runTasks("Externalizing final round", tasks);
			
			// If distributed ..
			PDQConfig config = PDQConfig.current();
			if (config.isDistributable() && config.getNumWorkers() > 1) {
				Object[][] remotePartialMaps;
				int numThreads = config.getThreads();
				int numWorkers = config.getNumWorkers();
				int numDelegates = delegates.size();
				int rank = PDQ.rank();
				
				if (rank == 0) {
					logger.info("waiting to recv all");
					Object[] allDelegates = PDQ.recvAll();
					allDelegates[0] = delegates;
					
					Object[][][] data =
							new Object[numWorkers][numThreads][numWorkers];
					
					for (int i = 0; i < numWorkers; i++) {
						List<Map<K, V>> dlg = (List<Map<K, V>>) allDelegates[i];
						for (int j = 0; j < numWorkers; j++) {
							for (int k = 0; k < numThreads; k++) {
								data[j][k][i] = dlg.get(j*numThreads + k);
							}
						}
					}
					
					PDQ.sendAll(data);
					remotePartialMaps = data[0];
				} else {
					// If slave.. send my PartialMaps to the master
					PDQ.sendTo(0, delegates);
					
					remotePartialMaps = (Object[][]) PDQ.recvFrom(0);
				}
				
				logger.info("now to common code");
				// remove partials this rank is not responsible for
				for (int i = 0; i < numWorkers; i++) {
					for (int j = 0; j < numThreads; j++) {
						if (PDQ.rank() != i)
							delegates.set(i * numThreads + j, null);
					}
				}
				
				// add the remote partial maps into our list
				for (int i = 0; i < numThreads; i++) {
					PartialMap partialMap = 
							(PartialMap) delegates.get(rank*numThreads + i);
					for (int j = 0; j < numWorkers; j++) {
						if (j == rank) continue; // don't add my local partial map
						partialMap.add((PartialMap) remotePartialMaps[i][j]);
					}
				}
			}
			
			for (int i = 0; i < partitions; i ++) {
				if (! isDelegatePartial(i)) continue;
				PartialMap partialMap = (PartialMap) delegates.get(i);
				Callable<Long> task = partialMap.compact();
				if (task != null) tasks.add(task);
			}
			
			runTasks("Merging external maps", tasks);
			
			for (int i = 0; i < partitions; i ++) {
				if (! isDelegatePartial(i)) continue;
				PartialMap partialMap = (PartialMap) delegates.get(i);
				partialMap.cleanup();
				delegates.set(i, partialMap.getCurrent());
			}
			
			
			if (config.isDistributable() && config.getNumWorkers() > 1) {
				int rank = PDQ.rank();
				int numThreads = config.getThreads();
				int numWorkers = config.getNumWorkers();
				
				if (rank == 0) {
					Object[] remoteDelegates = PDQ.recvAll();
					for (int i = 1; i < numWorkers; i++) {
						List<Map<K, V>> dlg = 
								(List<Map<K, V>>) remoteDelegates[i];
						for (int j = 0; j < numThreads; j++) {
							delegates.set(i * numThreads + j, 
									dlg.get(i * numThreads + j));
						}
					}
				} else {
					PDQ.sendTo(0, delegates);
				}
				
			}
		}
	}
	
	private boolean isDelegatePartial(int index) {
		if (delegates.get(index) == null) return false;
		return PartialMap.class.isAssignableFrom(delegates.get(index).getClass());
	}
	
	void runTasks(String description, List<Callable<Long>> callables) throws InterruptedException {
		if (callables.size() == 0) return;

		ExecutorService exec = PDQ.getExecutorService();
				
		if (exec == null)
			throw new IllegalStateException("merging when not in thread context");

		
		List<Future<Long>> results = exec.invokeAll(callables);
		exec.awaitTermination(0, TimeUnit.SECONDS);

		
		if (logger.isInfoEnabled()) {
			int nresults = results.size();

			long[] durations = new long[nresults];
			for (int i = 0; i < nresults; i++) {
				try {
					durations[i] = results.get(i).get();
				} catch (ExecutionException e) {
					e.printStackTrace();
					durations[i] = -1L;
				}
			}

			logger.info(String.format("%s in %s", description,
					Arrays.toString(durations)));
		}
		
		if (PDQConfig.current().isGarbageCollect())
			System.gc();
		
		callables.clear();
	}
	
	
	@SuppressWarnings("unchecked")
	@Override
	public Iterator<?> getSplits() {
		List<Iterator<Object>> iterators = new ArrayList<Iterator<Object>>();
		
		for (int i = 0; i < partitions; i++) {
			Map<K, V> map = delegates.get(i);
			if (map instanceof Splittable) {
				iterators.add((Iterator<Object>) ((Splittable) map).getSplits());
			} else {
				iterators.add(new FireOnceIterator<Object>(map));
			}
		}
		
		return new WrappedIterator<Object>(
				iterators, 
				false);
	}
	
	@Override
	public String toString() {
		return delegates.toString();
	}
	
	@SuppressWarnings("unchecked")
	int compare(K first, K second) {
		if (comparator != null) {
			return comparator.compare(first, second);
		} else {
			return ((Comparable<K>) first).compareTo(second);
		}
	}
	
	static final class PutAllCallable<K, V> implements Callable<Long> {
		ExternalSortedMap<K, V> emap;
		Map<? extends K, ? extends V> m;
		
		PutAllCallable(ExternalSortedMap<K, V> emap, 
				Map<? extends K, ? extends V> m) {
			this.emap = emap;
			this.m = m;
		}
		
		@Override
		public Long call() throws Exception {
			long start = System.currentTimeMillis();
			
			emap.putAll(m);
			emap.flush();
			emap.close();
			m.clear();
			emap = null;
			m = null;
			
			return System.currentTimeMillis() - start;
		}
	}
	
	final class PartialMap extends AbstractMap<K, V> implements Serializable {
		private static final long serialVersionUID = 3348869134687861582L;
		private final List<Map<K, V>> onDiskMaps = new ArrayList<Map<K, V>>();
		private Map<K, V> current;
		
		PartialMap(Map<K, V> map) {
			current = map;
		}
		
		Map<K, V> getCurrent() {
			return current;
		}
		
		void add(PartialMap second) {
			onDiskMaps.addAll(second.onDiskMaps);
		}
		
		@SuppressWarnings("unchecked")
		Callable<Long> createMergeTask(Object[] maps) throws IOException {
			if (maps.length == 0) return null;
			
			// If the current delegate is empty, no need to merge
			// other maps to this empty map. Just copy one of the
			// maps that have to be merged.
			if (current.size() == 0) {
				int i;
				int len = maps.length;
				for (i = 0; i < len; i++) {
					if (maps[i] != null) {
						current = (Map<K, V>) maps[i];
						maps[i] = null;
						i++;
						break;
					}
				}
				if (i == len) return null;
			}
			
			
			if (isMultimap()) {
				if (current instanceof ExternalSortedMap) {
					maps = Arrays.copyOf(maps, maps.length + 1);
					maps[maps.length - 1] = current;
					
					current = newExternalMap(false);
					
					return new MergeSortedMultimap<K, V>(
							(SortedMultimap<K, V>) current,
							comparator,
							maps);	
				} else {
					return new MergeMultimap<K, V>(
							(Multimap<K, V>) current,
							maps);
				}
			} else {
				if (current instanceof ExternalSortedMap) {
					maps = Arrays.copyOf(maps, maps.length + 1);
					maps[maps.length - 1] = current;
					
					current = newExternalMap(false);
					
					return new MergeSortedMap<K, V>(
							(SortedMap<K, V>) current,
							comparator,
							adder,
							maps);
				} else {
					return new MergeMap<K, V>(
							current,
							adder,
							maps);
				}
			}	
		}
		
		Callable<Long> externalize(boolean force) throws IOException {
			// No need to dump, if it is already on disk
			if (current instanceof ExternalSortedMap)
				return null;

			// No need to dump an empty map
			if (current.size() == 0) return null;
			
			if (force || (onDiskMaps.size() > 0)) {
				ExternalSortedMap<K, V> external = newExternalMap(true);
				onDiskMaps.add(external);
				return new PutAllCallable<K, V>(external, current);
			}
			
			return null;
		}
		
		/**
		 * 
		 * @return
		 * @throws IOException
		 */

		Callable<Long> compact() throws IOException {
			// Nothing to do here
			if (onDiskMaps.size() == 0) return null;
			
			if (current instanceof ExternalSortedMap)
				onDiskMaps.add(current);

			// If there is only one map to merge; no need to 
			// actual merge it. Just copy is over
//			if (onDiskMaps.size() == 1) {
//				current = onDiskMaps.get(0);
//				onDiskMaps.clear();
//				return null;
//			}
			
			current = newExternalMap(false);
			
			if (isMultimap()) {
				return new MergeSortedMultimap<K, V>(
						(SortedMultimap<K, V>) current, 
						comparator,  
						onDiskMaps.toArray());	
			} else {
				return new MergeSortedMap<K, V>(
						(SortedMap<K, V>) current, 
						comparator, 
						adder, 
						onDiskMaps.toArray());
			}
			
		}
		
		/**
		 * 
		 * @throws IOException
		 */
		void cleanup() throws IOException {
			if (current instanceof ExternalSortedMap)
				((ExternalSortedMap<K, V>) current).flush();
			
			for (Map<K, V> map: onDiskMaps) {
				map.clear();
			}
			onDiskMaps.clear();
		}
		
		/**
		 * 
		 */
		@Override
		public Set<java.util.Map.Entry<K, V>> entrySet() {
			return null;
		}
		
		@Override
		public String toString() {
			return onDiskMaps.toString();
		}
	}
}