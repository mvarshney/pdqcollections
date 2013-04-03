package pdqninja.pdqio;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;

import pdqninja.pdq.PDQConfig;
import pdqninja.pdq.Splittable;

public class PDQFileInputStream extends InputStream implements Splittable, Serializable {
	private static final long serialVersionUID = 2607506068983257403L;
	
	/**
	 * The files that the PDQFileInputStream will read.
	 */
	private final Collection<? extends File> files;
	
	/**
	 * Splitter object used to split files.
	 */
	private final Splitter splitter;
	
	/**
	 * Iterator for the files.
	 */
	transient private Iterator<? extends File> filesIterator;
	
	/**
	 * FileReader for the currently opened file.
	 */
	transient private FileInputStream currentInputStream = null;

	private long blocksize;
	
	/**
	 * Creates a PDQFileInputStream for the specified file and 
	 * splitter.
	 * 
	 * @param file the specified file
	 * @param splitter the splitter to split the file in chunks
	 * @throws IOException
	 */
	public PDQFileInputStream(File file, Splitter splitter) throws IOException {
		this(Arrays.asList(file), splitter);
	}
	
	/**
	 * Create a PDQFileInputStream for the specified collection of files
	 * and splitter.
	 * 
	 * @param files the specified collection of files
	 * @param splitter the splitter to split the file in chunks
	 * @throws IOException
	 */
	public PDQFileInputStream(Collection<? extends File> files,
			Splitter splitter) throws IOException {
		blocksize = PDQConfig.current().getBlocksize();
		this.files = files;
		this.filesIterator = files.iterator();
		openNextFile();
	
		this.splitter = splitter;
	
	}
	
	boolean openNextFile() throws IOException {
		if (currentInputStream != null)
			currentInputStream.close();
		
		if (filesIterator.hasNext()) {
			currentInputStream = new FileInputStream(filesIterator.next());
			return true;
		}
		
		return false;
	}
	
	@Override
	public int read() throws IOException {
		int b = currentInputStream.read();
		if (b == -1 && openNextFile()) {
			return read();
		}
		
		return b;
	}
	
	/* (non-Javadoc)
	 * @see java.io.InputStream#read(byte[], int, int)
	 */
	@Override
	public int read(byte[] b, int off, int len) throws IOException {
		int bytesRead = currentInputStream.read(b, off, len);
		if (bytesRead == -1 && openNextFile()) {
			return read(b, off, len);
		}
		
		return bytesRead;
	}

	/* (non-Javadoc)
	 * @see java.io.InputStream#close()
	 */
	@Override
	public void close() throws IOException {
		// close the reader
		if (currentInputStream != null)
			currentInputStream.close();
		currentInputStream = null;

		// drain the iterator
		while (filesIterator.hasNext())
			filesIterator.next();
	}

	@Override
	public Iterator<?> getSplits() {
		try {
			FileStoreIterator fsIt = FileStores.getDiskStorage(files);
			return new SplitIterator(fsIt, blocksize, splitter);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	
	static final class SplitIterator extends FileSplitIterator<FileInputStream> {
		SplitIterator(FileStoreIterator fsIterator, long blocksize,
				Splitter splitter) throws IOException {
			super(fsIterator, blocksize, splitter);
		}

		@Override
		FileInputStream createSplit(RandomAccessFile raf, long length)
				throws IOException {
			return new FileSplitInputStream(raf, length);
		}	
	}

	private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
		in.defaultReadObject();
		
		filesIterator = files.iterator();
	}
	
	private void writeObject(ObjectOutputStream out) throws IOException {
		out.defaultWriteObject();
	}



}
