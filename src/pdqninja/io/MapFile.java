package pdqninja.io;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.io.Externalizable;
import java.io.Serializable;
import java.io.StreamCorruptedException;

/**
 * A file that stores a sequence of keys (and optionally, the values).
 * <p>
 * An <tt>ExternalMapFile</tt> stores a sequence of keys, or a pair
 * or key and values, in a flat file. The objects are stored
 * in their serialized form, which means that these objects
 * must implement the {@link Serializable} or the {@link Externalizable}
 * interface. The objects are read and written using the 
 * {@link ObjectInputStream#readUnshared()} and 
 * {@link ObjectOutputStream#writeUnshared(Object)} methods.
 * While writing to file, the object output stream is 
 * <em>reseted</em> after every <tt>bytesPerReset</tt> bytes. 
 * <p>
 * The file can be opened in a read-only or read-write modes.
 * The <tt>mode</tt> parameter in the constructor determines
 * the mode of file: "r" opens the file in read-only mode, while
 * "rw" opens the file in read-write mode. In the read-write mode,
 * file will be created if it does not exists. If the file does
 * exists, the file pointer will be located at the first byte.
 * <p>
 * If the file is opened in read-write mode, this object can 
 * also create an <em>index</em> for the file. An entry in the
 * index is a pair of key and the offset within the file where
 * this key is written. One index entry is created every 
 * <tt>bytesPerIndex</tt> bytes.
 * <p>
 * This object buffers data while reading and writing by using 
 * the {@link BufferedInputStream} and {@link BufferedOutputStream},
 * respectively. The size of the buffer can be specified in the
 * constructor arguments. When the {@link #seek(long)} method 
 * is called, the existing contents of the read buffer are discarded.
 * Therefore, a calling function can safely seek to an offset and
 * starting reading from there. Although, it is the responsiblity
 * of the calling function to ensure that seeked offset is a valid
 * position from where the ObjectInputStream can correctly read
 * and parse data.
 * <p>
 * Objects are read from the file with the {@link #read()} method
 * which returns the next object stored in the file. The objects
 * are written with the {@link #write(Object)} or the
 * {@link #write(Object, Object)} methods. The first method
 * writes a key only, and the second methods writes a key and a value.
 * 
 * 
 * @author mvarshney
 */
public class MapFile<K> {
	public static final long DEFAULT_BYTES_PER_RESET = 8196L;
	public static final int DEFAULT_BUFFER_SIZE = 8196 * 1024;
	
	private final long bytesPerIndex;
	private final long bytesPerReset;
	
	private final MapFileIndex<K> index;
	private final RandomAccessFile raf;
	
	private final BufferedInputStream bis;
	private final ObjectInputStream ois;
	
	private final ObjectOutputStream oos;
	private final CountableOutputStream cos;
	
	private long nextResetAt = -1;
	private long nextIndexAt = -1;
	
	/**
	 * Creates an ExternalMapFile object of the specified file
	 * and open it in the specified mode. 
	 * The valid modes are "r" (read only) and "rw" (read and write).
	 * If the specified mode
	 * is "rw" (read-write mode), a new file will be created if
	 * it did not exist. If the file exists, the file pointer
	 * will be located at the first byte. This object will not
	 * create an <em>index</em> for the file.
	 * 
	 * @param file the file for reading/writing data
	 * @param mode the mode in which to open the file 
	 * @throws IOException
	 */
	public MapFile(File file, String mode) throws IOException {
		this(file, mode, null, 0, 
				DEFAULT_BYTES_PER_RESET, 
				DEFAULT_BUFFER_SIZE);
	}
	
	/**
	 * Creates an ExternalMapFile object of the specified file
	 * and open it in the specified mode, using the specified 
	 * buffer size and resetting the stream after every specified
	 * number of bytes (for write mode). 
	 * The valid modes are "r" (read only) and "rw" (read and write).
	 * If the specified mode
	 * is "rw" (read-write mode), a new file will be created if
	 * it did not exist. If the file exists, the file pointer
	 * will be located at the first byte. This object will not
	 * create an <em>index</em> for the file.
	 * 
	 * @param file the file for reading/writing data
	 * @param mode the mode in which to open the file
	 * @param bytesPerReset number of bytes after which 
	 * the ObjectOutputStream is resetted, or zero to use the
	 * default value
	 * @param bufferSize the size of the buffers in BufferedInputStream
	 * and BufferedOutputStream, or zero to use the default value
	 * 
	 * @throws IOException
	 */
	public MapFile(File file, String mode, 
			long bytesPerReset, int bufferSize) throws IOException {
		this(file, mode, null, 0, bytesPerReset, bufferSize);
	}
	
	/**
	 * Creates an ExternalMapFile object of the specified file
	 * and open it in the specified mode, using the specified index
	 * object to store indices, using the specified 
	 * buffer size and resetting the stream after every specified
	 * number of bytes (for write mode). 
	 * The valid modes are "r" (read only) and "rw" (read and write).
	 * If the specified mode
	 * is "rw" (read-write mode), a new file will be created if
	 * it did not exist. If the file exists, the file pointer
	 * will be located at the first byte. If the index object is
	 * not <tt>null</tt> and <tt>bytesPerReset</tt> is greater
	 * than zero, this object will create index for the file. 
	 * 
	 * @param file the file for reading/writing data
	 * @param mode the mode in which to open the file
	 * @param index the index object
	 * @param bytesPerIndex number of bytes after which a
	 * new index entry is created 
	 * @param bytesPerReset number of bytes after which 
	 * the ObjectOutputStream is resetted, or zero to use the
	 * default value
	 * @param bufferSize the size of the buffers in BufferedInputStream
	 * and BufferedOutputStream, or zero to use the default value
	 * @throws IOException
	 */
	public MapFile(File file, String mode,
			MapFileIndex<K> index, long bytesPerIndex,
			long bytesPerReset, int bufferSize) throws IOException {
		
		this.bytesPerIndex = bytesPerIndex;
		
		if (bytesPerIndex <= 0)
			this.index = null;
		else 
			this.index = index;
		
		if (bytesPerReset <= 0)
			this.bytesPerReset = bytesPerReset;
		else 
			this.bytesPerReset = DEFAULT_BYTES_PER_RESET;
		
		if (bufferSize <= 0)
			bufferSize = DEFAULT_BUFFER_SIZE;
		
		if (mode.equals("r")) {
			raf = new RandomAccessFile(file, "r");
			cos = null;
			oos = null;
		} else if (mode.equals("rw")) {
			raf = new RandomAccessFile(file, "rw");	
			
			cos = new CountableOutputStream(
					new BufferedOutputStream(
							new FileOutputStream(raf.getFD()),
							bufferSize));
			
			/* The writeStreamHeader method of the ObjectOutputStream 
			 * is overridden to disable writing header. This action
			 * is paired with overriding the readStreamHeader in the
			 * ObjectInputStream (see few lines below) to disable
			 * reading header (of course, since no header was
			 * written). This allows us the ability to seek anywhere within
			 * the file and start reading from there. 
			 */
			oos = new ObjectOutputStream(cos) {
				@Override
				protected void writeStreamHeader() throws IOException {
				}
			};
		} else {
			throw new IllegalArgumentException("");
		}
		
		FileInputStream fis = new FileInputStream(raf.getFD());
		
		/* The reset() method of the BufferedInputStream is
		 * overridden here. When this method is called, the
		 * existing data in the buffer is discarded. That is,
		 * the next read() will load the data freshly from 
		 * the underlying InputStream. Note that we have 
		 * potentially violated the contract of the reset() method,
		 * as this method is meant to reset the 'marks' in the stream.
		 * However, this is an anonymous inner class, and we
		 * create the underlying FileInputStream stream, so it
		 * is invisible to the outside.
		 * We need this for the seek() method. When we seek to
		 * a different offset and start reading, the problem is
		 * that we will still get the existing data in the buffer,
		 * which was read from the older location. Discarding
		 * buffer solves this problem.
		 * 
		 */
		bis = new BufferedInputStream(fis, bufferSize) {
			@Override
			public synchronized void reset() throws IOException {
				count = 0;
				pos = 0;
			}
			
		};
		
		/* Overridden readStreamHeader method to disable reading
		 * header. See notes above, where the ObjectOutputStream
		 * was overridden.
		 */
		ois = new ObjectInputStream(bis) {
			@Override
			protected void readStreamHeader() throws IOException,
					StreamCorruptedException {
			}
		};
	}
	
	
	/**
	 * Returns the index object used by this object.
	 * @return the index object
	 */
	public MapFileIndex<K> getIndex() {
		return index;
	}

	/**
	 * Returns the length of file in number bytes.
	 * 
	 * @return the length of file in bytes
	 * @throws IOException
	 */
	public long length() throws IOException {
		return raf.length();
	}
	
	/**
	 * Seek to the specified offset
	 * @param offset the offset to seek
	 * @throws IOException
	 */
	public void seek(long offset) throws IOException {
		raf.seek(offset);
		bis.reset();
	}
	
	/**
	 * Read the next object in the file.
	 * 
	 * @return the next object in the file
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	public Object read() throws IOException, ClassNotFoundException {
		return ois.readUnshared();
	}
	
	/**
	 * Write the specified key to the file.  
	 * 
	 * @param key the object to write to the file
	 * @throws IOException
	 */
	public void write(K key) throws IOException {
		if (oos == null)
			throw new IOException("ExternalMapFile is opened in read only mode");
		checkResetAndIndex(key);
		oos.writeUnshared(key);
	}
	
	/**
	 * Write the specified key and value to the file.
	 * 
	 * @param key the key object to write
	 * @param value the value object to write
	 * @throws IOException
	 */
	public void write(K key, Object value) throws IOException {
		if (oos == null)
			throw new IOException("ExternalMapFile is opened in read only mode");
		checkResetAndIndex(key);
		oos.writeUnshared(key);
		oos.writeUnshared(value);
	}
	
	/**
	 * Flushes the output stream.
	 * 
	 * @throws IOException
	 */
	public void flush() throws IOException {
		if (oos == null)
			throw new IOException("ExternalMapFile is opened in read only mode");
		oos.flush();
	}
	
	/**
	 * Closes the file.
	 * 
	 * @throws IOException
	 */
	public void close() throws IOException {
		if (oos != null) oos.close();
		if (ois != null) ois.close();
		
		raf.close();
	}
	
	/**
	 * Checks if the stream needs to be reseted, and a new
	 * index entry has to be created.  
	 * The stream is reseted if more than <tt>bytesPerReset</tt> bytes
	 * have been written since last reset.
	 * An index entry is created if this object
	 * is configured to create indices, and more than
	 * <tt>bytesPerIndex</tt> bytes have been written since last 
	 * index entry.
	 * 
	 * @param key the key for the index entry will be created, if it
	 * is decided to create a new index entry
	 * @throws IOException
	 */
	void checkResetAndIndex(K key) throws IOException {
		long offset = cos.getBytesWritten();
		
		if (index != null) {
			if (offset > nextIndexAt) {
				oos.reset();
				offset = cos.getBytesWritten();
				nextResetAt = offset + bytesPerReset;

				index.createIndex(key, offset);
				nextIndexAt = offset + bytesPerIndex;
			} else {
				index.skip();
			}
		}
		
		if (offset > nextResetAt) {
			oos.reset();
			nextResetAt = offset + bytesPerReset;
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException {
		if (args.length < 1) {
			System.out.println("Usage: ... <filename>");
			return;
		}
		
		for (String arg: args) {
			File file = new File(arg);
			MapFile<Object> mapfile = new MapFile<Object>(file, "r");

			System.out.println(">>>> " + arg);
			try {
				Object k = mapfile.read();
				Object v = mapfile.read();
				System.out.println(String.format("%s\t%s\n", k, v));
			} catch (EOFException e) {

			} catch (FileNotFoundException e) {
				System.out.println("\tFile not found.");
			}
		}
	}
}
