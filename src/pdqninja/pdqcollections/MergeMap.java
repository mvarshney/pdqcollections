package pdqninja.pdqcollections;

import java.util.Map;
import java.util.concurrent.Callable;

import pdqninja.pdq.Adder;

final class MergeMap<K, V> implements Callable<Long> {
	Object[] maps;
	Map<K, V> dest;
	Adder<V> adder; 
	
	MergeMap(Map<K, V> dest, Adder<V> adder, Object[] maps) {
		this.dest = dest;
		this.maps =  maps;
		this.adder = adder;
	}

	@Override
	public Long call() throws Exception {
		long start = System.currentTimeMillis();
		
		for (Object obj: maps) {
			if (obj == null) continue;
			@SuppressWarnings("unchecked")
			Map<K, V> map = (Map<K, V>) obj;
			
			for (Map.Entry<K, V> entry : map.entrySet()) {
				K key = entry.getKey();
				V value = dest.get(key);

				if (value == null) {
					dest.put(entry.getKey(), entry.getValue());
				} else {
					dest.put(entry.getKey(), adder.add(value, entry.getValue()));
				}
			}
			
			map.clear();
		}
	
		
		long duration = System.currentTimeMillis() - start;
	
		
		maps = null;
		dest = null;
		adder = null;
		return duration;
	}
	
}
