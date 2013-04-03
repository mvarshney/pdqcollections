package pdqninja.pdqio;

import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * Splits a file into fixed sized chunks. The size
 * of the chunk is equal to the <em>blocksize</em>.
 * <p>
 * Use the {@link #FixedSizeSplitter()} constructor to
 * evenly divide the file into chunks of blocksize. Use
 * the {@link #FixedSizeSplitter(long)} constructor to 
 * evenly divide the file into
 * chunks of blocksize <em>after</em> skipping 
 * <code>initialSkip</code> bytes in the beginning.
 * Use the {@link #FixedSizeSplitter(long, long)}
 * constructor to evenly divide the file into
 * chunks of blocksize <em>after</em> skipping 
 * <code>initialSkip</code> bytes in the beginning, and 
 * ensuring that the chunk ends at record boundary, where each
 * record has a length of <code>recordLen</code>.
 * 
 * @author mvarshney
 *
 */
public class FixedSizeSplitter implements Splitter {
	private final long initialSkip;
	private final long recordLen;
	
	/**
	 * Creates a splitter that evenly divides the file
	 * into chunks of blocksize.
	 */
	public FixedSizeSplitter() {
		this(0, 1);
	}
	
	/**
	 * Creates a splitter that evenly divides the file into
	 * chunks of blocksize <em>after</em> skipping 
	 * <code>initialSkip</code> bytes in the beginning.
	 * 
	 * @param initialSkip number of bytes to skip in the
	 * beginning of the file
	 */
	public FixedSizeSplitter(long initialSkip) {
		this(initialSkip, 1);
	}
	
	/**
	 * Creates a splitter that evenly divides the file into
	 * chunks of blocksize <em>after</em> skipping 
	 * <code>initialSkip</code> bytes in the beginning, and 
	 * ensuring that the chunk ends at record boundary, where each
	 * record has a length of <code>recordLen</code>.
	 * 
	 * @param initialSkip number of bytes to skip in the
	 * beginning of the file
	 * @param recordLen size of each record
	 */
	public FixedSizeSplitter(long initialSkip, long recordLen) {
		this.initialSkip = initialSkip;
		this.recordLen = recordLen;
	}
	
	/**
	 * Locates the beginning offset of the next split, or
	 * return <code>-1</code> if there are no more splits.
	 */
	@Override
	public long splitBegin(RandomAccessFile raf) throws IOException {
		long size = raf.length();
		long current = raf.getFilePointer();
		
		if (current == 0)
			current += initialSkip;
		
		if (current >= size)
			return -1;
		
		return current;
	}

	/**
	 * Locates the end offset of the next split.
	 */
	@Override
	public long splitEnd(RandomAccessFile raf, long blocksize)
			throws IOException {
		long current = raf.getFilePointer();
		long size = raf.length();
		
		if (current >= size)
			return -1;
	
		long end = current + blocksize;
		while ((end % recordLen) != 0)
			end ++;
		
		if (end > size)
			end = size;
		
		return end;
	}

}
