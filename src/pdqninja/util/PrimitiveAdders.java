package pdqninja.util;

import java.io.Serializable;
import java.util.Collection;
import pdqninja.pdq.Adder;

/**
 * Adder classes for common primitive datatypes.
 * 
 * @author mvarshney
 *
 */
public final class PrimitiveAdders {
	private PrimitiveAdders() {}
	
	static class ShortAdder implements Adder<Short>, Serializable {
		/**
		 * 
		 */
		private static final long serialVersionUID = 7148758728552467309L;

		@Override
		public Short add(Short first, Short second) {
			return (short) (first + second);
		}
	}
	
	static class IntegerAdder implements Adder<Integer>, Serializable {
		/**
		 * 
		 */
		private static final long serialVersionUID = 1666726051502820150L;

		@Override
		public Integer add(Integer first, Integer second) {
			return (first + second);
		}
	}
	
	static class LongAdder implements Adder<Long>, Serializable {
		/**
		 * 
		 */
		private static final long serialVersionUID = -1175514054350472549L;

		@Override
		public Long add(Long first, Long second) {
			return (first + second);
		}
	}
	
	static class FloatAdder implements Adder<Float>, Serializable {
		/**
		 * 
		 */
		private static final long serialVersionUID = 7575288612980706350L;

		@Override
		public Float add(Float first, Float second) {
			return (first + second);
		}
	}
	
	static class DoubleAdder implements Adder<Double>, Serializable {
		/**
		 * 
		 */
		private static final long serialVersionUID = -4976091465035651979L;

		@Override
		public Double add(Double first, Double second) {
			return (first + second);
		}
	}
	
	static class CollectionsAdder<E> implements Adder<Collection<E>> {

		@Override
		public Collection<E> add(Collection<E> first, Collection<E> second) {
			first.addAll(second);
			return first;
		}
	}
	
	/**
	 * Adds two <code>Short</code> objects.
	 */
	public static final Adder<Short> ShortAdder = new ShortAdder();
	
	/**
	 * Adds two <code>Integer</code> objects.
	 */
	public static final Adder<Integer> IntegerAdder = new IntegerAdder();
	
	/**
	 * Adds two <code>Long</code> objects.
	 */
	public static final Adder<Long> LongAdder = new LongAdder();
	
	/**
	 * Adds two <code>Float</code> objects.
	 */
	public static final Adder<Float> FloatAdder = new FloatAdder();
	
	/**
	 * Adds two <code>Double</code> objects.
	 */
	public static final Adder<Double> DoubleAdder = new DoubleAdder(); 
			
	
	/**
	 * 
	 * 
	 * @param cls datatype class
	 * @return Adder for the class, or <code>null</code> if
	 * there is no known adder for the class
	 */
	@SuppressWarnings("unchecked")
	public static <E> Adder<E> getAdder(Class<E> cls) {
		if (Short.class.isAssignableFrom(cls)) {
			return (Adder<E>) ShortAdder;
		} else if (Integer.class.isAssignableFrom(cls)) {
			return (Adder<E>) IntegerAdder;
		} else if (Long.class.isAssignableFrom(cls)) {
			return (Adder<E>) LongAdder;
		} else if (Float.class.isAssignableFrom(cls)) {
			return (Adder<E>) FloatAdder;
		} else if (Double.class.isAssignableFrom(cls)) {
			return (Adder<E>) DoubleAdder;
		}
		
		return null;
	}
}
