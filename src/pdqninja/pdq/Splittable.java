package pdqninja.pdq;

import java.util.Iterator;

/**
 * A class implements the <code>Splittable</code> interface to indicate that
 * it can be divided into smaller instances.
 * <p>
 * The cornerstones of the PDQ computation model is the ability to 
 * divide input data into smaller chunks that can be 
 * processed efficiently, and subsequently, the ability to merge smaller 
 * chunks of output to create complete data.
 * The PDQ programming model further requires that the splitting of 
 * input, as well as merging of output, must be the responsibility of
 * respective data structures, which frees the programmer from the need of
 * incorporating this computation model in their code. 
 * <p>
 * The <code>Splittable</code> interface
 * indicates that an object can be splitted into smaller sizes. 
 * A splittable class implements the {@link #getSplits()} method that
 * returns an Iterator for the splits. 
 * <p>
 * The PDQ library will always call the <tt>getSplits</tt> function from
 * a sequential context, therefore, the implementing class does not
 * have to ensure thread-safety in this method.
 * 
 * @author mvarshney
 * @see Mergeable
 */
public interface Splittable {
	/**
	 * Returns an iterator for the splits of this object.
	 * <p>
	 * The PDQ library asks a splittable object to deliver splits. It is
	 * the responsibility of the implementing class to manage the lifetime
	 * of the returned object.
	 * <p>
	 * The class does not have to implement the {@link Iterator#remove()}
	 * method for the returned Iterator. The PDQ library will always call 
	 * this method from
	 * a sequential context, therefore, the implementing class does not
	 * have to ensure thread-safety in this method.
	 * 
	 * @return an iterator for the splits of this object
	 */
	Iterator<?> getSplits();
}
