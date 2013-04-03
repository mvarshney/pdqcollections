package pdqninja.pdq;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.ExecutorService;

/**
 * Access to the 
 * @author mvarshney
 *
 */
public final class PDQ {
	private PDQ() {}
	
	static private PDQPrivate priv;
	static PDQWorker worker;
	
	public static void run(Object object, Object... args) throws NoSuchMethodException, RuntimeException, IllegalAccessException, InvocationTargetException, InterruptedException, IOException {
		run(object, "default", args);
	}
	
	public static void run(Object object, String name, Object... args) throws NoSuchMethodException, RuntimeException, IllegalAccessException, InvocationTargetException, InterruptedException, IOException {
		run(object.getClass(), object, name, 0, 1, args);
	}
	
	public static void run(Class<?> cls, Object... args) 
			throws NoSuchMethodException, RuntimeException, 
			InvocationTargetException, 
			InterruptedException, IOException {
		run(cls, "default", args);
	}
	
	public static void run(Class<?> cls, String name, Object... args) 
			throws NoSuchMethodException, RuntimeException, 
			InvocationTargetException, 
			InterruptedException, IOException {
		run(cls, null, name, 0, 1, args);
	}
	
	
	static void run(Class<?> cls, Object object, String name, 
			int rank, int numWorkers,
			Object... args) 
			throws NoSuchMethodException, RuntimeException, 
			InvocationTargetException, 
			InterruptedException, IOException {
		
		String programName = cls.getSimpleName() + "." + name;
		
		priv = new PDQPrivate(programName, rank, numWorkers);
		priv.run(cls, object, name, args);
		priv.teardown();
		priv = null;
	}
	
	public static ExecutorService getExecutorService() {
		if (priv == null)
			throw new IllegalStateException("This method can be called " +
					"from PDQ execution context only");
		return priv.getExecutorService();
		
	}
	
	public static int rank() {
		if (priv == null)
			throw new IllegalStateException("This method can be called " +
					"from PDQ execution context only");
		
		return priv.rank();
	}
	
	public static void sendTo(int rank, Object data) {
		if (priv == null)
			throw new IllegalStateException("This method can be called " +
					"from PDQ execution context only");
		
		try {
			worker.oos.writeUnshared(data);
			worker.oos.flush();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	public static void sendAll(Object[] data) {
		if (priv == null)
			throw new IllegalStateException("This method can be called " +
					"from PDQ execution context only");
		
		priv.sendAll(data);
	}
	
	public static Object recvFrom(int rank) {
		if (priv == null)
			throw new IllegalStateException("This method can be called " +
					"from PDQ execution context only");
		Object data = null;
		
		try {
			data = worker.ois.readObject();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return data;
	}
	
	public static Object[] recvAll() {
		if (priv == null)
			throw new IllegalStateException("This method can be called " +
					"from PDQ execution context only");
		
		return priv.recvAll();
	}
}
