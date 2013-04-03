package pdqninja.pdqcollections;

import java.util.Iterator;
import java.util.Map;

final class PQEntry<K, V> {
	Iterator<Map.Entry<K, V>> it;
	Map.Entry<K, V> entry;
	
	PQEntry(Iterator<Map.Entry<K, V>> it, int index) {
		this.it = it;
	}
	
	boolean next() {
		if (it.hasNext()) {
			entry = it.next();
			return true;
		} else {
			return false;
		}
	}
	
	@Override
	public String toString() {
		return entry.toString();
	}
}