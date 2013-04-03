package pdqninja.pdqio;

import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.CharBuffer;

/**
 * Creates a FileReader for a chunk within a file. The FileReader
 * will begin reading where the chunk starts, and will return
 * <tt>EOF</tt> where the chunk ends.
 * 
 * @author mvarshney
 *
 */
class FileSplitReader extends FileReader {
	long remaining;
	final RandomAccessFile raf;
	
	/**
	 * Creates a FileChunkReader for the specified RandomAccessFile,
	 * where the chunk starts at the current offset of <tt>raf</tt>
	 * and has a size of <tt>chunkSize</tt>.
	 *  
	 * @param raf the specified RandomAccessFile
	 * @param chunkSize the size of the chunk
	 * @throws IOException
	 */
	FileSplitReader(RandomAccessFile raf, long chunkSize) throws IOException {
		super(raf.getFD());
		this.raf = raf;
		this.remaining = chunkSize;
	}


	/**
	 * {@inheritDoc}
	 */
	@Override
	public int read() throws IOException {
		int n = -1;
		if (remaining > 0) {
			n = super.read();
			remaining --;
			if (n == -1) {
				remaining = 0;
			}
		}
		
		return n;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int read(char[] cbuf, int offset, int length) throws IOException {
		if (remaining <= 0) return -1;
		
		long toread = remaining;
		if (toread > length)
			toread = length;

		int n = super.read(cbuf, offset, (int) toread);
		
		if (n == -1) {
			remaining = 0;
		} else {
			remaining -= n;
		}
		return n;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int read(CharBuffer target) throws IOException {
		if (remaining <= 0) return -1;
		
		int len = target.remaining();
		if (len > remaining)
			len = (int) remaining;
		
		char[] cbuf = new char[len];
		int n = read(cbuf, 0, len);
		if (n > 0)
			target.put(cbuf, 0, n);
		return n;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int read(char[] cbuf) throws IOException {
		return read(cbuf, 0, cbuf.length);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public long skip(long n) throws IOException {
		if (remaining <= 0) return 0;
		
		long skipped = super.skip(n);
		remaining -= skipped;
		return skipped;
	}


	/**
	 * {@inheritDoc}
	 */
	@Override
	public void close() throws IOException {
		super.close();
		if (raf != null) {
			raf.close();
		}
	}
	
	
}
