package pdqninja.pdq;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Indicates that the argument will be shared across the multiple
 * parallel or distributed instantiations of the computation.
 * 
 * <p>
 * The methods that have been annotated as @{@link Parallel} can optionally
 * annotate their arguments as <code>&#64;Shared</code> to indicate that these
 * arguments are shared across the different threads or processes.
 * 
 * <p>If the shared object is mutable, it is the responsibility of the programmer 
 * to properly ensure that it is safely accessed by different threads (for example, 
 * by synchronizing access to the shared resources, using concurrent data 
 * structures, etc).
 * <p> 
 * In distributed execution mode, the shared objects are serialized and
 * transmitted to {@link PDQWorker}s on the remote machines. If running
 * in distributed mode, ensure that the shared objects are serializable.
 * Note also that whereas the shared objects are sent to 
 * the workers they are never received back during or after the computation is 
 * finished. So a computation that relies on the visibility of changes 
 * made by one thread to shared objects at the other threads will probably
 * not work correctly in the distributed mode.  
 * <p>
 * The annotation must not be applied to the first argument of the method. 
 *  
 * @author mvarshney
 *
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.PARAMETER)
public @interface Shared {

}
