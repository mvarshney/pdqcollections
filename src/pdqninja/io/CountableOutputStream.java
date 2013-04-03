package pdqninja.io;

import java.io.IOException;
import java.io.OutputStream;

/**
 * An OutputStream that counts the number of bytes written.
 * The total number of bytes written so far can be
 * retrieved via the {@link #getBytesWritten()} method.
 * <p>
 * This class uses the <em>Decorator</em> design pattern, that is,
 * an object of this class contains the actual output stream object.
 * Before calling any of the <tt>write()</tt> methods of the 
 * underlying output stream object, this object counts the
 * number of bytes requested to be written.
 * 
 * @author mvarshney
 *
 */
public class CountableOutputStream extends OutputStream {
	private final OutputStream os;
	private long offset;
	
	/**
	 * Creates an CountableOutputStream object with the
	 * specified OutputStream object as the underlying 
	 * output stream.
	 * 
	 * @param os the underlying output stream output
	 */
	public CountableOutputStream(OutputStream os) {
		this.os = os;
		this.offset = 0L;
	}
	
	/**
	 * Returns the number of bytes written by the underlying
	 * output stream object.
	 * 
	 * @return number of bytes written
	 */
	long getBytesWritten() {
		return offset;
	}
	
	@Override
	public void write(byte[] b) throws IOException {
		os.write(b);
		offset += b.length;
	}

	@Override
	public void write(byte[] b, int off, int len) throws IOException {
		os.write(b, off, len);
		offset += len;
	}

	@Override
	public void flush() throws IOException {
		os.flush();
	}

	@Override
	public void close() throws IOException {
		os.close();
	}

	@Override
	public void write(int b) throws IOException {
		os.write(b);
		offset ++;
	}
	
}
