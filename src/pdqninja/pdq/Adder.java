package pdqninja.pdq;

/**
 * Adds two objects.
 * <p>
 * When merging certain data structures (such as {@link PDQMap}), it is 
 * required that their elements must be added. A class implementing
 * the <code>Adder</code> interface is used by the PDQ library to perform
 * the additions.
 * <p>
 * Note that for the correct behavior of the PDQ computation model, it is
 * required that the addition operation is both commutative (that is, A + B = 
 * B + A) and associative (that is, (A + B) + C = A + (B + C)). As an example,
 * <code>String</code> cannot be "added", since it is not a commutative operation. 
 * <p>
 * Adder objects for common primitives data types are available in 
 * {@link com.pdqninja.util.PrimitiveAdders}.
 * 
 * @author mvarshney
 *
 * @see com.pdqninja.util.PrimitiveAdders
 */
public interface Adder<E> {
	/**
	 * Adds two objects.
	 * 
	 * @param first first object to add
	 * @param second second object to add
	 * @return object that is the addition of first and second
	 */
	E add(E first, E second);
}
