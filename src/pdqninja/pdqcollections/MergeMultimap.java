package pdqninja.pdqcollections;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Callable;

import pdqninja.util.Multimap;


final class MergeMultimap<K, V> implements Callable<Long> {
	Multimap<K, V> dest;
	Object[] maps;
	
	MergeMultimap(Multimap<K, V> dest, Object[] maps) {
		this.dest = dest;
		this.maps = maps;
	}

	@Override
	public Long call() {
		long start = System.currentTimeMillis();
		
		for (Object obj: maps) {
			if (obj == null) continue;
			
			@SuppressWarnings("unchecked")
			Multimap<K, V> from = (Multimap<K, V>) obj;
			for (Map.Entry<K, Collection<V>> entry : from.allEntrySet()) {
				K key = entry.getKey();
				Collection<V> value = dest.getAll(key);
				
				if (value == null) {
					dest.put(key, entry.getValue());
				} else {
					value.addAll(entry.getValue());
				}
			}
			
			from.clear();
		}
		
		long duration = System.currentTimeMillis() - start;
		return duration;
	}
	
}
