package pdqninja.pdq;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;


/**
 * Indicates that the method is intended to be executed by the PDQ library 
 * (via the {@link PDQ}.run() methods). 
 * <p>
 * The annotation roughly emulates "function pointers", a capability not 
 * present in Java. Although the typical pattern to create function 
 * references is via Functors, they have a limitation that the function 
 * signature (number and type of arguments and return value) must be
 * predefined. This annotation overcomes this limitation to provide an easy
 * way to reference methods where the number and type of arguments can vary.
 * <p>
 * The annotation can be applied to instance 
 * methods as well as class static methods. The method is allowed to throw 
 * exceptions.
 * <p>
 * The annotated methods must follow the following requirements:
 * <li>The method must have public visibility.</li>
 * <li>The return value of the method is ignored by the PDQ library. 
 * It is recommended that the return type be <code>void</code>.</li>
 * <li>The method must have at least one argument, and the first argument
 * must not be annotated as @{@link Shared}</li>
 * <li>The first argument must be input.</li>
 * <li>All arguments not annotated by @{@link Shared} must be
 * regular objects, that is, they cannot be primitive types.</li>
 * 
 * <p>
 * Note that it is the responsibility of the programmer to ensure that
 * no two annotated methods in the same class have the same {@link #name()}.
 * This includes annotated methods declared in super classes.
 *   
 * @author mvarshney
 *
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface Parallel {
	/**
	 * Unique name for the method.
	 * <p>
	 * Note that it is the responsibility of the programmer to ensure that
	 * no two annotated methods in the same class have the same {@link #name()}.
	 * This includes annotated methods declared in super classes.
	 */
	String name() default "default";
}
