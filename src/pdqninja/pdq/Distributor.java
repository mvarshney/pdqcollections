package pdqninja.pdq;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.NotSerializableException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.reflect.Array;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


/**
 * Coordinate with {@link PDQWorker}s to distribute jobs and collect results.
 * <p>
 * 
 * @author mvarshney
 *
 */
class Distributor {
	static final int SocketBufferSize = 256 * 1024; 
			
	static final int DEFAULT_WORKER_PORT = 9760;
	int numWorkers = 1;
	List<Socket> sockets = new ArrayList<Socket>();
	List<ObjectInputStream> inputStreams = new ArrayList<ObjectInputStream>();
	List<ObjectOutputStream> outputStreams = new ArrayList<ObjectOutputStream>();
	
	/**
	 * Connect to each worker. The list of workers is presented as
	 * a comma separated string of hostname and optionally the port number.
	 * Establishes a connection with each worker and sends a "register"
	 * message.
	 *  
	 * @param workerStr list of workers
	 */
	void connect(String workerStr) {
		String[] workers = workerStr.split(",");
		for (String worker: workers) {
			String[] hostPort = worker.split(":");
			try {
				InetAddress address = InetAddress.getByName(hostPort[0]);
				int port = hostPort.length > 1 ? Integer.parseInt(hostPort[1]) : DEFAULT_WORKER_PORT;
				Socket sock = new Socket();
				sock.connect(new InetSocketAddress(address, port));
				sockets.add(sock);

				
				ObjectOutputStream oos =
						new ObjectOutputStream(
								new BufferedOutputStream(
										sock.getOutputStream(),
										SocketBufferSize));
				
				oos.flush();
				outputStreams.add(oos);
				
				inputStreams.add(
						new ObjectInputStream(
								new BufferedInputStream(
										sock.getInputStream(),
										SocketBufferSize)));
				
				//logger.info("Connected to " + address);
				
				numWorkers ++;
			} catch (UnknownHostException e) {
				e.printStackTrace();
				continue;
			} catch (IOException e) {
				e.printStackTrace();
				continue;
			}
		}
		
		// Send register message to all output streams
		MessageRegister register = new MessageRegister(
				0, numWorkers, PDQConfig.current().clone());
		
		int rank = 1;
		//register.config.setWorkers(null);
		
		for (ObjectOutputStream oos: outputStreams) {
			register.rank = rank;
		
			try {
				oos.writeUnshared(register);
				oos.flush();
			} catch (IOException e) {
				e.printStackTrace();
			}
				
			rank ++;
		}
	}
	
	void disconnect() {
		for (Socket sock: sockets) {
			try {
				sock.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	int getNumWorkers() {
		return numWorkers;
	}
	
	
	void assignWork(Object object, String name, Object... args) throws IOException {
		assignWork(new MessageRequest(object, name, args));
	}
	
	void assignWork(Class<?> cls, String name, Object... args) throws IOException {
		assignWork(new MessageRequest(cls, name, args));
	}
	
	void assignWork(MessageRequest request) throws IOException {
		request.wrap();
		Object[] data = new Object[numWorkers];
		Arrays.fill(data, request);
		sendAll(data);
	}
	
	void sendAll(Object[] data) {
		Thread[] threads = new Thread[numWorkers];
		for (int i = 1; i < numWorkers; i++) {
			threads[i] = new WriteThread(data[i], outputStreams.get(i - 1));
			threads[i].start();
		}
		
		for (int i = 1; i < numWorkers; i++) {
			try {
				threads[i].join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
	void sendTo(int rank, Object data) {
		ObjectOutputStream oos = outputStreams.get(rank - 1);
		try {
			oos.writeObject(data);
			oos.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	Object[] recvAll() {
		Object[] data = new Object[numWorkers];
		ReadThread[] threads = new ReadThread[numWorkers];
		
		for (int i = 1; i < numWorkers; i++) {
			threads[i] = new ReadThread(inputStreams.get(i - 1));
			threads[i].start();
		}
		
		for (int i = 1; i < numWorkers; i++) {
			try {
				threads[i].join();
				data[i] = threads[i].getData();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		return data;
	}
	
	Object recvFrom(int rank) {
		Object data = null;
				
		try {
			data = inputStreams.get(rank - 1).readObject();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return data;
	}
	
	static final class WriteThread extends Thread {
		final Object data;
		final ObjectOutputStream oos;
		
		public WriteThread(Object data, ObjectOutputStream oos) {
			this.data = data;
			this.oos = oos;
		}
		
		@Override
		public void run() {
			try {
				oos.writeObject(data);
				oos.flush();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
	}
	
	static final class ReadThread extends Thread {
		final ObjectInputStream ois;
		Object data;
		
		ReadThread(ObjectInputStream ois) {
			this.ois = ois;
		}
		
		@Override
		public void run() {
			try {
				data = ois.readObject();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		Object getData() {
			return data;
		}
	}
	
	public Object[][] gatherResults(int nargs) throws InterruptedException {
		Object[][] workerResults = new Object[nargs][numWorkers - 1];
		
		Object[] results = recvAll();
		
		for (int worker = 1; worker < numWorkers; worker++) {
			MessageArguments msg = (MessageArguments) results[worker];
			for (int i = 1; i < nargs; i++) {
				workerResults[i][worker - 1] = msg.args[i];
			}
		}
		
		return workerResults;
	}
	

	/**
	 * Registration message.
	 * 
	 * @author mvarshney
	 *
	 */
	static final class MessageRegister implements Serializable {
		private static final long serialVersionUID = 1L;
		int rank;
		int numWorkers;
		PDQConfig config;
		
		public MessageRegister(int rank, int numWorkers,
				PDQConfig config) {
			this.rank = rank;
			this.numWorkers = numWorkers;
			this.config = config;
		}
	}
	

	/**
	 * Message containing arguments.
	 * 
	 * @author mvarshney
	 *
	 */
	static class MessageArguments implements Serializable {
		private static final long serialVersionUID = -5405111949307694345L;
		Object[] args;
		
		MessageArguments(Object[] args) {
			this.args = Arrays.copyOf(args, args.length);
		}
	}
	
	/**
	 * Work assignment message.
	 * 
	 * @author mvarshney
	 *
	 */
	static final class MessageRequest extends MessageArguments {
		private static final long serialVersionUID = 4692814522076704949L;

		enum Type {
			SERIALIZED_CLASS,
			SERIALIZED_OBJECT,
			CONSTRUCTED_OBJECT
		}
		Type type;
		Class<?> cls;
		Object object;
		String name;
		
		public MessageRequest(Class<?> cls, String name, Object[] args) throws NotSerializableException {
			super(args);
			this.cls = cls;
			this.object = null;
			this.name = name;
			
		}
		
		public MessageRequest(Object object, String name, Object[] args) throws NotSerializableException {
			super(args);
			this.cls = null;
			this.object = object;
			this.name = name;
		}
		
		void wrap() throws NotSerializableException {
			if (object == null) {
				type = Type.SERIALIZED_CLASS;
			} else {
				cls = object.getClass();
				
				if (object instanceof Serializable) {
					type = Type.SERIALIZED_OBJECT;
				} else {	
					object = null;
					type = Type.CONSTRUCTED_OBJECT;
				}
			}
			
			
		}
		
		void unwrap() throws InstantiationException, IllegalAccessException, NotSerializableException, FileNotFoundException  {
			if (type == Type.CONSTRUCTED_OBJECT) {
				object = cls.newInstance();
			}
			
		}
	}
}
