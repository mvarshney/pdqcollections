package pdqninja.pdq;

import java.lang.reflect.Method;
import java.util.concurrent.Callable;

final class ThreadPoolCallable implements Callable<Long> {
	private Method method;
	private Object object;
	private Object[] args;
	
	public ThreadPoolCallable(Object object, Method method, Object[] args) {
		this.method = method;
		this.object = object;
		this.args = args;
	}

	@Override
	public Long call() throws Exception {
		long start = System.currentTimeMillis();
		
		try {
			method.invoke(object, args);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return System.currentTimeMillis() - start;
	}
	
	Object getArg(int index) {
		return args[index];
	}
}