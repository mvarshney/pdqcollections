package pdqninja.pdqio;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.io.Reader;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import pdqninja.pdq.PDQConfig;
import pdqninja.pdq.Splittable;

/**
 * 
 * @author mvarshney
 *
 */
public class PDQFileReader extends Reader implements Splittable, Serializable {
	
	private static final long serialVersionUID = -6093922434714383969L;
		
	/**
	 * The files that the PDQFileReader will read.
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
	transient private FileReader currentReader = null;

	private long blocksize;

	
	/**
	 * Creates a PDQFileReader for the specified file and
	 * default {@link Splitter}.
	 * @param file the specified file
	 * @throws IOException
	 */
	public PDQFileReader(File file) throws IOException {
		this(Arrays.asList(file), null);
	}
	
	/**
	 * Creates a PDQFileReader for the specified file and 
	 * splitter.
	 * 
	 * @param file the specified file
	 * @param splitter the splitter to split the file in chunks
	 * @throws IOException
	 */
	public PDQFileReader(File file, Splitter splitter) throws IOException {
		this(Arrays.asList(file), splitter);
	}
	
	/**
	 * Create a PDQFileReader for the specified collection of files
	 * and the default {@link Splitter}.
	 * 
	 * @param files the specified collection of files
	 * @throws IOException
	 */
	public PDQFileReader(Collection<? extends File> files) throws IOException {
		this(files, null);
	}
	
	/**
	 * Create a PDQFileReader for the specified collection of files
	 * and splitter.
	 * 
	 * @param files the specified collection of files
	 * @param splitter the splitter to split the file in chunks
	 * @throws IOException
	 */
	public PDQFileReader(Collection<? extends File> files,
			Splitter splitter) throws IOException {
		blocksize = PDQConfig.current().getBlocksize();
		this.files = files;
		this.filesIterator = files.iterator();
		openNextFile();
		
		if (splitter == null) {
			this.splitter = DelimitedSplitter.NewLineDelimitedSplitter;
		} else {
			this.splitter = splitter;
		}
	}
	
	boolean openNextFile() throws IOException {
		if (currentReader != null)
			currentReader.close();
		
		if (filesIterator.hasNext()) {
			currentReader = new FileReader(filesIterator.next());
			return true;
		}
		
		return false;
	}
	
	@Override
	public int read(char[] cbuf, int off, int len) throws IOException {
		int bytesRead = currentReader.read(cbuf, off, len);
		if (bytesRead == -1 && openNextFile()) {
			return read(cbuf, off, len);
		}
		
		return bytesRead;
	}

	@Override
	public void close() throws IOException {
		// close the reader
		if (currentReader != null)
			currentReader.close();
		currentReader = null;
		
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

	static final class SplitIterator extends FileSplitIterator<FileReader> {
		SplitIterator(FileStoreIterator fsIterator, long blocksize,
				Splitter splitter) throws IOException {
			super(fsIterator, blocksize, splitter);
		}

		@Override
		FileReader createSplit(RandomAccessFile raf, long length)
				throws IOException {
			return new FileSplitReader(raf, length);
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
