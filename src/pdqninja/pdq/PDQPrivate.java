package pdqninja.pdq;

import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

/**
 * Implements the Split-Replicate-Merge computation model.
 * 
 * @author mvarshney
 *
 */
final class PDQPrivate {
	static private Logger logger = Logger.getLogger(PDQ.class);
	
	private final PDQConfig config;
	private final ExecutorService exec;
	private Distributor distributor;
	private int rank;
	private int numWorkers;
	private long computationStartedAt;
	
	/**
	 * Creates the computation object with the specified programName.
	 * The <tt>numWorkers</tt> is the number of workers participating
	 * in the computation. This is greater than one in distributed 
	 * execution mode. The <tt>rank</tt> is the index of this JVM
	 * amongst the distributed participants. The master JVM
	 * always has the rank of <tt>0</tt> and the rank of other JVMs
	 * range from 1 to numWorkers.
	 * <p>
	 * At the master JVM, this object is created each time the 
	 * Split-Replicate-Merge computation is initiated via one of the
	 * <tt>PDQ.run()</tt> methods. At the worker JVMs, this object is
	 * created in response to work assignment message from the master.
	 * <p>
	 * The actual computation starts by calling the 
	 * {@link #run(Class, Object, String, Object...)} method. 
	 * This method will return when the computation completes, after
	 * which the object can be cleaned up via the {@link #teardown()}
	 * method.
	 * 
	 * 
	 * @param programName name of the program
	 * @param rank the rank of this JVM
	 * @param numWorkers the number of JVMs participating in this
	 * computation
	 */
	PDQPrivate(String programName, int rank, int numWorkers) {
		this.config = PDQConfig.current();
		this.exec = Executors.newFixedThreadPool(config.getThreads());
		this.rank = rank;
		this.numWorkers = numWorkers;
		
		if (logger.isDebugEnabled()) {
			logger.debug(String.format("Started computation [%s]. Rank=%d", 
					programName, rank));
		}
		
		if ((rank == 0) 
				&& config.getWorkers() != null
				&& ! config.getWorkers().equals("")) {
			distributor = new Distributor();
			distributor.connect(config.getWorkers());
			this.numWorkers = distributor.getNumWorkers();
			logger.info("Distributor created. Num workers = " + this.numWorkers);
			
		}
	}
	
	/**
	 * Cleanup this computation object.
	 * @throws InterruptedException
	 */
	void teardown() throws InterruptedException {
		if (exec != null) {
			exec.shutdown();
			exec.awaitTermination(0, TimeUnit.SECONDS);
		}

		if (distributor != null) {
			distributor.disconnect();
		}
		
		if (logger.isInfoEnabled()) {
			logger.info("Computation completed in " + 
					(System.currentTimeMillis() - computationStartedAt));
		}
	}
	
	/**
	 * Returns the current ThreadPoolExecutorService object.
	 * @return the current ThreadPoolExecutorService object, or 
	 * <tt>null</tt> if this object has been cleaned up already 
	 */
	ExecutorService getExecutorService() {
		return exec;
	}
	
	/**
	 * Execute the method defined in the specified
	 * class (if the method is static) or the specified object. 
	 * The <tt>name</tt> is the tag defined in the {@link Parallel}
	 * annotation (or "<em>default</em>", if not defined). The
	 * <tt>args</tt> are the arguments to the method.
	 * <p>
	 * This method verifies that the method is present and its
	 * arguments are consistent with the provided arguments. 
	 * After verification checks, this methods assigns the work
	 * to the workers (if running in distributed mode), and calls
	 * the {@link #run(Object, Method, Object...)} method
	 * to actually execute the computation.
	 * 
	 * @param cls the class where the method is defined
	 * @param object the object on which to invoke the method,
	 * or <tt>null</tt> if the method is static
	 * @param name the name defined in the Parallel annotation
	 * @param args the arguments to the method
	 * 
	 * @throws NoSuchMethodException
	 * @throws RuntimeException
	 * @throws InvocationTargetException
	 * @throws InterruptedException
	 * @throws IOException
	 */
	void run(Class<?> cls, Object object, String name, Object... args) 
			throws NoSuchMethodException, RuntimeException, 
			InvocationTargetException, 
			InterruptedException, IOException {

		computationStartedAt = System.currentTimeMillis();
		
		// Walk through all the methods in this class...
		for (Method m: cls.getMethods()) {
			
			Parallel annotation = m.getAnnotation(Parallel.class);
			
			// Check if this method has @Parallel annotation
			if (annotation == null) continue;

			// Check if this annotation has the correct name
			if (! annotation.name().equalsIgnoreCase(name))
				continue;

			// If object is null, verify this is a static method			
			if (object == null) {
				int modifier = m.getModifiers();
				if (! Modifier.isStatic(modifier)) continue;
			} 
			
			// Check if number of arguments provided are
			// consistent with the arguments defined in the method
			int nargs = args.length;
			int expectedArgs = m.getParameterTypes().length;
			
			if (nargs == 0) {
				throw new IllegalArgumentException("No arguments given");
			}
			
			if (nargs != expectedArgs) {
				String msg = "Method " + m.getName() + 
						" expects " + expectedArgs +
						" parameters but "; 
				if (nargs == 0) {
					msg += "no arguments are given";
				} else if (nargs == 1) {
					msg += ((nargs < expectedArgs) ? "only " : "") + 
							"one argument is given";
				} else {
					msg += ((nargs < expectedArgs) ? "only " : "") +
							nargs + " arguments are given";
				}
				throw new IllegalArgumentException(msg);
			}
			
			// Everything looks good now...
			
			// If running in distributed mode, assign this computation
			// to the workers
			if (distributor != null) {
				long start = System.currentTimeMillis();
				if (object == null)
					distributor.assignWork(cls, name, args);
				else
					distributor.assignWork(object, name, args);
		
				long duration = System.currentTimeMillis() - start;
				logger.info("Distributed work in " + duration);
			}

			// Run the computation..
			run(object, m, args);
			return;
		}
		throw new NoSuchMethodException(name);
	}
	
	/**
	 * Executes the specified method in Split-Replicate-Merge
	 * computation model. If the <tt>object</tt> is not null,
	 * the method is invoked on the specified object, otherwise,
	 * the method is assumed to be static.
	 * 
	 * 
	 * @param object the object on which the method is invoked,
	 * or <tt>null</tt> if the method is static
	 * @param method the method to execute
	 * @param args the arguments to the method
	 * 
	 * @throws RuntimeException
	 * @throws InvocationTargetException
	 * @throws InterruptedException
	 * @throws IOException
	 */
	private void run(Object object, Method method, Object... args) 
			throws RuntimeException, 
			InvocationTargetException, InterruptedException, IOException {	

		int numThreads = config.getThreads();
		int nargs = args.length;
		
		/* Identify the @Shared parameters. Walk through all
		 * parameters of this method and check which ones are
		 * specified with the @Shared annotation. The  
		 * <tt>sharedArgs<tt> array records those indices of
		 * the paramters that are Shared.
		 */
		boolean sharedArgs[] = new boolean[nargs];
		Annotation[][] annotations = method.getParameterAnnotations();
		
		for (int i = 0; i < nargs; i++) {
			sharedArgs[i] = false;
			Annotation[] tmp = annotations[i];
			for (int j = tmp.length - 1; j >= 0; j--) {
				if (tmp[j] instanceof Shared) {
					sharedArgs[i] = true;
				}
			}
		}
		
		// First parameter cannot be Shared (since the first parameter
		// MUST be an input argument)
		if (sharedArgs[0]) {
			throw new IllegalArgumentException("First parameter cannot be shared");
		}
		
		// Time to run this in parallel
		int threads;
		int iteration = 0;
		ThreadPoolCallable[] callables = new ThreadPoolCallable[numThreads];
		Iterator<?> splits = ((Splittable) args[0]).getSplits();
		
		// In distributed mode, each worker is responsible for splits 
		// in following order:
		// [rank0] [rank1] ... [rankN] [rank0] ... 
		
		// Skip the first rank number of splits
		for (int i = 0; i < rank && splits.hasNext(); i++)
			splits.next();
		
		while (true) {
			long start = System.currentTimeMillis();
			
			for (threads = 0; threads < numThreads; threads++) {
				Object[] threadArgs = new Object[nargs]; 
				
				// Split the input
				if (! splits.hasNext()) break;
				
				threadArgs[0] = splits.next();
				
				// Skip one cycle of splits (for distributed mode only)
				for (int i = 1; i < numWorkers && splits.hasNext(); i++)
					splits.next();
				
				// Prepare the other arguments (shared and replicates)
				for (int i = 1; i < nargs; i++) {
					if (sharedArgs[i]) {
						// If argument is @Shared, use the same object
						threadArgs[i] = args[i];
					} else {
						// Otherwise, create a Replicate and use that
						threadArgs[i] = ((Mergeable) args[i]).replicate();
					}
				}
				
				callables[threads] = new ThreadPoolCallable(object, method, threadArgs);
			}
			
			if (threads == 0) break;
			
			long splitTime = System.currentTimeMillis() - start;
			
			// Run threads
			List<Future<Long>> result = exec.invokeAll(Arrays.asList(callables).subList(0, threads));
			
			long execTime = System.currentTimeMillis() - start - splitTime;
			
			// Merge output
			for (int i = 1; i < nargs; i++) {
				if (sharedArgs[i]) continue;
				
				Object[] partials = new Object[threads];
				for (int threadId = 0; threadId < threads; threadId++) {
					partials[threadId] = callables[threadId].getArg(i);
				}
				
				((Mergeable) args[i]).merge(partials);
			}

			// Print some statistics
			
			if (logger.isInfoEnabled()) {
				long end = System.currentTimeMillis();
				long total = end - start;
				long mergeTime = total - execTime - splitTime;

				iteration ++;

				int nresults = result.size();
				long[] durations = new long[nresults];
				for (int i = 0; i < nresults; i++) {
					try {
						durations[i] = result.get(i).get();
					} catch (ExecutionException e) {
						e.printStackTrace();
						durations[i] = -1L;
					}
				}

				logger.info(String.format("Iteration#%d. Split=%d Exec=%d %s Merge=%d. Total=%d",
						iteration, splitTime, execTime, Arrays.toString(durations), mergeTime, total));
				printStat();
			}
			
			if (config.isGarbageCollect())
				System.gc();
		}
		
		// All iterations are done now... call the merge() function
		// one more time with <tt>null</tt> argument to indicate
		// to the Mergeables that there won't be any more mergers
		for (int i = 1; i < nargs; i++) {
			if (sharedArgs[i]) continue;
			((Mergeable) args[i]).merge((Object[]) null);
		}

		// If running in distributed mode, collect the results from the
		// workers (this will run on the master JVM only)
//		if (distributor != null) {
//			long start = System.currentTimeMillis();
//			Object[][] workerResults = distributor.gatherResults(nargs);
//			
//			logger.info("Distributed Gathering results from " +
//					numWorkers + " workers = " + 
//					(System.currentTimeMillis() - start));
//			
//			// Ask the Mergeables to merge these results gathered by
//			// workers
//			for (int i = 1; i < nargs; i++) {
//				if (sharedArgs[i]) continue;
//				((Mergeable) args[i]).merge(workerResults[i]);
//				((Mergeable) args[i]).merge((Object[]) null);				
//			}
//		}
	}
	
	
	
	static final SimpleDateFormat dateFormat = new SimpleDateFormat("dd-MM H:mm:ss");

	int rank() {
		return rank;
	}

	void sendTo(int rank, Object data) {
		if (distributor == null) return;
		distributor.sendTo(rank, data);
	}
	
	void sendAll(Object[] data) {
		if (distributor == null) return;
		distributor.sendAll(data);
	}
	
	Object recvFrom(int rank) {
		if (distributor == null) return null;
		return distributor.recvFrom(rank);
	}
	
	Object[] recvAll() {
		if (distributor == null) return null;
		return distributor.recvAll();
	}
	
	void printStat() {
		double total = 1.0 * Runtime.getRuntime().totalMemory() / 1024 / 1024;
		double free = 1.0 * Runtime.getRuntime().freeMemory() / 1024 / 1024;
		double used = total - free;
		
		System.out.println(String.format("##STAT## %s Total: %.2fMB Free: %.2fMB Used: %.2fMB",
				dateFormat.format(new Date()),
				total, free, used));
	}
}
