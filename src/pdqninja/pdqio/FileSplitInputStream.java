package pdqninja.pdqio;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * Creates an InputStream for a chunk within a file. The InputStream
 * will begin reading where the chunk starts, and will return
 * <tt>EOF</tt> where the chunk ends.
 * @author mvarshney
 *
 */
class FileSplitInputStream extends FileInputStream {
	long remaining;
	final RandomAccessFile raf;
	
	/**
	 * Creates a FileChunkInputStream for the specified RandomAccessFile,
	 * where the chunk starts at the current offset of <tt>raf</tt>
	 * and has a size of <tt>chunkSize</tt>.
	 * 
	 * @param raf the specified RandomAccessFile
	 * @param remaining the size of the chunk
	 * @throws IOException
	 */
	FileSplitInputStream(RandomAccessFile raf, long remaining) throws IOException {
		super(raf.getFD());
		this.raf = raf;
		this.remaining = remaining;
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
	public int read(byte[] b) throws IOException {
		return read(b, 0, b.length);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int read(byte[] b, int off, int len) throws IOException {
		if (remaining <= 0) return -1;

		long toread =  remaining;
		if (toread > len)
			toread = len;

		int n = super.read(b, off, (int) toread);

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
	public int available() throws IOException {
		int n = super.available();
		if (n > remaining)
			n = (int) remaining;
		return n;
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
