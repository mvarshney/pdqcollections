package pdqninja.pdqio;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.log4j.Logger;

/**
 * Generates splits for a collection of files in a 
 * {@link FileStoreIterator} by using a specified
 * {@link Splitter}.
 * <p>
 * This iterator fetch files from the FileStoreIterator
 * and use Splitter to create splits. When a file is
 * exhausted, it removes the file from the FileStoreIterator.
 * When all files are exhausted, this iterator terminates.
 * <p>
 * This iterator is an abstract class, and the overriding
 * classes must implement the only abstract function:
 * {@link #createSplit(RandomAccessFile, long)}. 
 * 
 * @author mvarshney
 */
abstract class FileSplitIterator<E> implements Iterator<E> {
	private static final Logger logger = Logger.getLogger(FileSplitIterator.class);
	
	private final FileStoreIterator fsIterator;
	private final long blocksize;
	private final Splitter splitter;
	private final Map<String, RandomAccessFile> openedRAFs =
			new HashMap<String, RandomAccessFile>();
	
	private RandomAccessFile currentRAF;
	private long currentLength;
	
	/**
	 * Creates a FileSplitIterator for a collection of files
	 * in the specified FileStoreIterator by splitting them
	 * using the specified Splitter and the specified
	 * blocksize.
	 * 
	 * @param fsIterator the FileStoreIterator
	 * @param blocksize blocksize of chunk
	 * @param splitter the Splitter used for splitting files
	 * @throws IOException
	 */
	FileSplitIterator(FileStoreIterator fsIterator,
			long blocksize, Splitter splitter) throws IOException {
		this.fsIterator = fsIterator;
		this.blocksize = blocksize;
		this.splitter = splitter;
		
		loadNextSplit();
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean hasNext() {
		return currentRAF != null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public E next() {
		if (! hasNext()) return null;
		
		E split = null;
		try {
			split = createSplit(currentRAF, currentLength);
			loadNextSplit();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return split;
	}

	/**
	 * This operation is not supported.
	 */
	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}

	/**
	 * Loads the next split in the current directory. This
	 * method will keep on trying for all the files in the
	 * current directory until it can find one split. The
	 * reasons why splitting a file may fail include:
	 * the file does not exists, the file is empty, the 
	 * end of the file is reached, there is I/O error
	 * in reading from the file. 
	 *  
	 * @throws IOException
	 */
	void loadNextSplitCurrentDir() throws IOException {
		Collection<File> files = fsIterator.getFiles();
	
		if (files == null || files.size() == 0)
			return;
		
		Iterator<File> it = files.iterator();
		while (it.hasNext()) {
			File file = it.next();
			RandomAccessFile raf = openedRAFs.get(file.getAbsolutePath());
			if (raf == null) {
				try {
					raf = new RandomAccessFile(file, "r");
				} catch (FileNotFoundException e) {
					//e.printStackTrace();
					// TODO: show warning that the file cannot be read
					it.remove();
					continue;
				}
				RandomAccessFile old = 
						openedRAFs.put(file.getAbsolutePath(), raf);
				if (old != null)
					old.close();
			}
			
			long begin = splitter.splitBegin(raf);
			if (begin == -1) {
				raf.close();
				it.remove();
				continue;
			}
			
			raf.seek(begin);
			
			long end = splitter.splitEnd(raf, blocksize);
			
			if (begin == end) {
				raf.close();
				it.remove();
				continue;
			}
			raf.seek(end);
			
			currentRAF = new RandomAccessFile(file, "r");
			currentRAF.seek(begin);
			currentLength = end - begin;
			
			logger.debug(String.format("Generated Split. %s %d %d",
					file.getAbsolutePath(), begin, end));
			
			return;
		}
	}
	
	/**
	 * Loads the next split. This method will try each directory
	 * until it can find one split. 
	 * @throws IOException
	 */
	void loadNextSplit() throws IOException {
		currentRAF = null;
		
		if (! fsIterator.hasNext()) return;

		// Go through the loop for one iteration loop cycle only
		int cycle = fsIterator.cycle();

		while (cycle > 0) {
			fsIterator.next();
			loadNextSplitCurrentDir();
			if (hasNext()) 
				break;
			cycle --;
		}
		
	}

	/**
	 * Creates a split for the RandomAccessFile that is already
	 * positioned at the beginning offset of the split. 
	 * 
	 * @param raf the RandomAccessFile
	 * @param length length of the split
	 * @return the split
	 * @throws IOException
	 */
	abstract E createSplit(RandomAccessFile raf, long length) throws IOException;
}
