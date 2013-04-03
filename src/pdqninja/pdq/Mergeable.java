package pdqninja.pdq;

import java.io.IOException;

/**
 * A class implements the <code>Mergeable</code> interface to indicate that
 * its complete form can be constructed from smaller instances.
 * <p>
 * The cornerstones of the PDQ computation model is the ability to 
 * divide input data into smaller chunks that can be 
 * processed efficiently, and subsequently, the ability to merge smaller 
 * chunks of output to create complete data.
 * The PDQ programming model further requires that the splitting of 
 * input, as well as merging of output, must be the responsibility of
 * respective data structures, which frees the programmer from the need of
 * rewriting their computation code. 
 * <p>
 * The <code>Mergeable</code> interface indicates that the object can be
 * constructed by merging multiple smaller instances. The process of merging
 * is as follows: first, the library asks for objects (via the
 * {@link #replicate()} method) that will serve as containers for storing
 * partial results; the library will execute some computation to process input
 * and store partial results in these container object; and finally, the library 
 * will ask the class to merge these containers back 
 * (via the {@link #merge(Object...)} method).
 * <p>
 * 
 * @author mvarshney
 * @see Splittable
 */
public interface Mergeable {
	/**
	 * Provide an object that can serve as a container for storing partial results.
	 * <p>
	 * The PDQ library can request a class implementing the Mergeable interface
	 * to generate these objects. This request can be made any number of times,
	 * and it is considered an error if <code>null</code> is returned.
	 * 
	 * @return object that can server as a container for storing partial results. 
	 */
	Object replicate();
	
	/**
	 * Merge partial results.
	 * <p>
	 * Note that the objects requested to be merged are the same objects that
	 * were generated via the {@link #replicate()} method.
	 * <p> 
	 * The PDQ library calls the <code>merge</code> function in the following
	 * manner: TODO
	 * 
	 * @param objects objects containing partial results
	 * @throws IOException
	 * @throws InterruptedException
	 */
	void merge(Object... objects) throws IOException, InterruptedException;
}
