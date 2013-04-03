package pdqninja.pdqcollections;

import java.io.Closeable;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.Callable;

import pdqninja.util.SortedMultimap;

final class MergeSortedMultimap<K, V> implements Callable<Long> {
	SortedMultimap<K, V> dest;
	Comparator<? super K> comparator;
	Object[] maps;
	
	MergeSortedMultimap(SortedMultimap<K, V> dest, Comparator<? super K> comparator,
			Object[] maps) {
		this.dest = dest;
		this.comparator = comparator;
		this.maps = maps;
	}
	
	Long copy() throws Exception {
		long start = System.currentTimeMillis();
		@SuppressWarnings("unchecked")
		SortedMultimap<K, V> map = (SortedMultimap<K, V>) maps[0];
		
		for (Map.Entry<K, Collection<V>> entry: map.allEntrySet()) {
			dest.put(entry.getKey(), entry.getValue());
		}
		

		if (dest instanceof Closeable) {
			((Closeable) dest).close();
		}
		
		dest = null;
		maps = null;
		
		return System.currentTimeMillis() - start;
	}
	
	@Override
	public Long call() throws Exception {
		if (maps == null || maps.length == 0) return 0L;
		
		if (maps.length == 1) return copy();
		
		
		long start = System.currentTimeMillis();
		
		PriorityQueue<PQEntry<K, Collection<V>>> pqueue;
		K lastKey = null;
		Collection<V> lastValue = null;
		
		pqueue = new PriorityQueue<PQEntry<K, Collection<V>>>(
				maps.length, new PQComparator<K, Collection<V>>((Comparator<? super K>) comparator));
		
		for (int i = 0; i < maps.length; i++) {
			@SuppressWarnings("unchecked")
			SortedMultimap<K, V> map = (SortedMultimap<K, V>) maps[i];
			if (map == null) continue;
			
			Iterator<Map.Entry<K, Collection<V>>> it = 
					map.allEntrySet().iterator();
			
			PQEntry<K, Collection<V>> entry = new PQEntry<K, Collection<V>>(it, i);
			if (entry.next()) {
				pqueue.add(entry);	
			}
		}
		
		while (true) {
			PQEntry<K, Collection<V>> top = pqueue.poll();
			if (top == null) break;
			if (top.entry.getKey().equals(lastKey)) {
				lastValue.addAll(top.entry.getValue());
			} else {
				if (lastKey != null) {
					dest.put(lastKey, lastValue);
				}
				lastKey = top.entry.getKey();
				lastValue = top.entry.getValue();
			}
			
			boolean more = top.next();
			if (more) {
				pqueue.add(top);
			}
			
		}
		dest.put(lastKey, lastValue);
		
		if (dest instanceof Closeable) {
			((Closeable) dest).close();
		}
		
		maps = null;
		dest = null;

		
		long duration = System.currentTimeMillis() - start;
		
		return duration;
		
	}

}
