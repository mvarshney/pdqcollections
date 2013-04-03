package pdqninja.pdqio;

import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * An object that can split a file into chunks. This object
 * is responsible for locating the begin and end of each
 * split within a given file.
 * <p>
 * The PDQ library uses the Splitter object in the following way:
 * <li>the {@link RandomAccessFile} will be positioned at a 
 * location which is the starting point for locating the next split.</li>
 * <li>the library will call {@link #splitBegin(RandomAccessFile)}
 * to request the file offset where the next split begins. The
 * Splitter object can return <code>-1</code> to indicate that
 * there are no more splits available</li>
 * <li>if the returned value is <code>-1</code>, the library
 * will not ask for any more splits</li>
 * <li>if the returned value is non-negative, the RandomAccessFile
 * will be positioned at that offset, and the library will call
 * the {@link #splitEnd(RandomAccessFile, long)} to request the end
 * of the split. The library provides <em>blocksize</em> as a hint
 * on the expected size of the split. The object must return a
 * valid file offset.</li>
 * <p>
 * The same splitter object can be asked to split more than
 * one file. It is recommended that the Splitter objects do not
 * cache any file specific information.
 *  
 * @author mvarshney
 *
 */
public interface Splitter {
	/**
	 * Locate the file offset where the next split begins, 
	 * or <code>-1</code> if there are no more splits.
	 * <p>
	 * The RandomAccessFile will already be positioned at
	 * a location after which the next split is requested.
	 * 
	 * @param raf the file for which the beginning offset of
	 * the next split is request
	 * @return file offset where the next split begins, 
	 * or <code>-1</code> if there are no more splits
	 * @throws IOException
	 */
	long splitBegin(RandomAccessFile raf) throws IOException;
	
	/**
	 * Locate the file offset where the next split ends.
	 * <p>
	 * The RandomAccessFile will be positioned at the offset
	 * returned by the previous call to {@link #splitBegin(RandomAccessFile)}.
	 * This method must return a valid file offset.
	 * 
	 * @param raf the file for which the end offset of
	 * the next split is request
	 * @param blocksize hint on the expected size of the split
	 * @return file offset where the next split ends
	 * @throws IOException
	 */
	long splitEnd(RandomAccessFile raf, long blocksize) throws IOException;
}
