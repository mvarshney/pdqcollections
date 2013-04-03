package pdqninja.pdq;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.InvocationTargetException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import pdqninja.pdq.Distributor.MessageArguments;
import pdqninja.pdq.Distributor.MessageRegister;
import pdqninja.pdq.Distributor.MessageRequest;


/**
 * Daemon process that runs on slave machines and performs
 * computation on behalf of a master.
 * <p>
 * 
 * @author mvarshney
 *
 */
public class PDQWorker extends Thread {
	final Socket socket;
	ObjectInputStream ois;
	ObjectOutputStream oos;
	
	PDQWorker(Socket socket) throws IOException {
		this.socket = socket;
	}
	
	@Override
	public void run()  {
		System.out.println("PDQWorker accepting work from " + 
				socket.getRemoteSocketAddress());
		
		try {
			ois = new ObjectInputStream(
							new BufferedInputStream(
							socket.getInputStream(),
							Distributor.SocketBufferSize));
			
			oos = new ObjectOutputStream(
							new BufferedOutputStream(
							socket.getOutputStream(),
							Distributor.SocketBufferSize));
			
			oos.flush();
			
			MessageRegister register = 
					(MessageRegister) ois.readUnshared();
			
			String localDir = PDQConfig.current().getLocalDir();
			PDQConfig.setCurrent(register.config);
			PDQConfig.current().setLocalDir(localDir);
			System.out.println("Setting local directory: " + localDir);

			// Wait for work request
			MessageRequest request = 
					(MessageRequest) ois.readUnshared();

			request.unwrap();

			System.out.println("Worker received work request");

			PDQ.worker = this;
			PDQ.run(request.cls, request.object, request.name, 
					register.rank, register.numWorkers, request.args);

			// Return result
//			request.args[0] = null;
//			MessageArguments result = 
//					new MessageArguments(request.args);
//			oos.writeUnshared(result);
//			oos.flush();
		} catch (EOFException e) {
			
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NoSuchMethodException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (RuntimeException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		try {
			socket.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		System.out.println("Worker completed work");
	}

	
	static void offerService(String[] args) throws IOException {
		int port = Distributor.DEFAULT_WORKER_PORT;
		
		for (String arg: args) {
			String[] keyval = arg.split("=", 2);
			if (keyval.length != 2) continue;
			
			if (keyval[0].equalsIgnoreCase("-port")) {
				port = Integer.parseInt(keyval[1]);
			}
		}
		
		ServerSocket server = new ServerSocket(port);
		server.setSoTimeout(2000);
		
		while (true) {
			try {
				Socket socket = server.accept();
				new PDQWorker(socket).start();
				
			} catch (SocketTimeoutException e) {
				if (Thread.currentThread().isInterrupted()) {
					break;
				} else {
					continue;
				}
			}
		}
	}
	
	public static void main(String[] args) throws IOException {
		BasicConfigurator.configure(new ConsoleAppender(new PatternLayout("%r [%t] %-5p %c{1} %x - %m%n")));
		Logger.getLogger("com.pdqninja").setLevel(Level.INFO);
		PDQConfig.current().fromArgs(args);
		offerService(args);
	}
}
