package pdqninja.pdqio;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.Serializable;

/**
 * Splits a file into chunks that are delimited by strings.
 * <p>
 * The chunk can be <em>end delimited</em>, that is, the chunk
 * ends with a specified string. An example is a newline delimited
 * chunk (each chunk ends at a newline). Use the 
 * {@link #DelimitedSplitter(String)} constructor to create 
 * such splitters.
 * <p>
 * The chunk can be <em> start and end delimited</em>, that is,
 * the chunk starts with a specified string and ends with another
 * specified string. An example is XML document, where each 
 * chunk would start with some opening XML tag, and end with 
 * corresponding closing tag. Use the 
 * {@link #DelimitedSplitter(String, String)} constructor to
 * create such splitters. Towards the end of the file, if the
 * splitter can locate the start delimiter but not the end
 * delimiter (because it reached the end of the file), this
 * object will still output this incomplete chunk (the offset of 
 * the end of the chunk will be the last byte offset of the file). 
 * 
 * @author mvarshney
 *
 */
public class DelimitedSplitter implements Splitter, Serializable {
	private static final long serialVersionUID = 1554205581990576505L;

	public static final DelimitedSplitter NewLineDelimitedSplitter = 
			new DelimitedSplitter("\n");
	
	private static final int BufferSize = 128;  
	private final String startDelim;
	private final String endDelim;
	
	/**
	 * Create a splitter that divides a file into chunks
	 * where each chunk terminates with <code>delim</code>.
	 * 
	 * @param delim string that indicates the end of a chunk
	 */
	public DelimitedSplitter(String delim) {
		this.startDelim = null;
		this.endDelim = delim;
	}
	
	/**
	 * Creates a splitter that divides a file into chunks
	 * where each chunk begins with <code>startDelim</code>
	 * and terminates with <code>endDelim</code>.
	 * 
	 * @param startDelim string that indicates the beginning of
	 * a chunk
	 * @param endDelim string that indicates the end of a chunk
	 */
	public DelimitedSplitter(String startDelim, String endDelim) {
		this.startDelim = startDelim;
		this.endDelim = endDelim;
	}
	
	
	long locateDelim(RandomAccessFile raf, String delim) throws IOException {
		long skipped = 0;
		int ch = 0;
		int matched = 0;
		int delimLen = delim.length();
		byte[] bytes = new byte[BufferSize];
		
		while (true) {
			int read = raf.read(bytes);
			if (read == -1) return skipped;
			
			for (int i = 0; i < read; i++) {
				ch = bytes[i];
				
				skipped ++;				
				if (ch == delim.charAt(matched)) {
					matched ++;
					if (matched == delimLen) {
						return skipped;
					}
				} else {
					matched = 0;
				}
			}
		}
	}
	
	/**
	 * Locates the beginning offset of the next split, or
	 * return <code>-1</code> if there are no more splits.
	 */
	@Override
	public long splitBegin(RandomAccessFile raf) throws IOException {
		long current = raf.getFilePointer();
		long size = raf.length();
		
		if (current >= size)
			return -1;
		
		if (startDelim != null) {
			if (current > 0) current++;
			current += locateDelim(raf, startDelim);
			if (current >= size)
				return -1;
			current -= startDelim.length();
		}
		
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
		long pos = current + blocksize;
		
		if (pos >= size) {
			return size;
		}
		
		raf.seek(pos);
		return pos + locateDelim(raf, endDelim);
	}

}
