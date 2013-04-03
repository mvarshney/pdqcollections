package pdqninja.pdqcollections;

import java.io.Closeable;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.SortedMap;
import java.util.concurrent.Callable;

import pdqninja.pdq.Adder;

final class MergeSortedMap<K, V> implements Callable<Long> {
	SortedMap<K, V> dest;
	Adder<V> adder; 
	Comparator<? super K> comparator;
	Object[] maps;
	
	MergeSortedMap(SortedMap<K, V> dest, Comparator<? super K> comparator,
			Adder<V> adder, Object[] maps) {
		this.dest = dest;
		this.comparator = comparator;
		this.adder = adder;
		this.maps = maps;
	}
	
	Long copy() throws Exception {
		long start = System.currentTimeMillis();
		@SuppressWarnings("unchecked")
		SortedMap<K, V> map = (SortedMap<K, V>) maps[0];
		
		for (Map.Entry<K, V> entry: map.entrySet()) {
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
		
		PriorityQueue<PQEntry<K, V>> pqueue;
		K lastKey = null;
		V lastValue = null;
		
		pqueue = new PriorityQueue<PQEntry<K, V>>(
				maps.length, new PQComparator<K, V>((Comparator<? super K>) comparator));
		
		for (int i = 0; i < maps.length; i++) {
			@SuppressWarnings("unchecked")
			SortedMap<K, V> map = (SortedMap<K, V>) maps[i];
			if (map == null) continue;
			
			Iterator<Map.Entry<K, V>> it = map.entrySet().iterator();
			PQEntry<K, V> entry = new PQEntry<K, V>(it, i);
			if (entry.next()) {
				pqueue.add(entry);	
			}	
		}

		while (true) {
			PQEntry<K, V> top = pqueue.poll();
			if (top == null) break;
			if (top.entry.getKey().equals(lastKey)) {
				lastValue = adder.add(lastValue, top.entry.getValue());
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
		adder = null;
		
		long duration = System.currentTimeMillis() - start;
		return duration;
	}

}
