package pdqninja.pdqio;

import java.io.File;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import pdqninja.util.HashMultimap;
import pdqninja.util.Multimap;
import pdqninja.util.WeightedLoopIterator;

/**
 * Iterates over directories in a <tt>File Store</tt>.
 * <p>
 * A File Store
 * is a set of directories that constitute one logical storage
 * division. As an example, on a computer with two hard drives
 * that are mounted at <tt>/</tt> and <tt>/mnt/disk2</tt>, one
 * may assign a temporary storage area consisting of these two
 * directories: <tt>/tmp</tt> and <tt>/mnt/disk2/tmp</tt>. In
 * this way, the I/O operations for temporary files can be spread
 * out across the two hard drives. Optionally, the different 
 * directories in a File Store can be assigned relative weights,
 * in which case, the I/O operations are divided across the
 * directories in that ratio. 
 * <p>
 * A FileStoreIterator is a {@link WeightedLoopIterator} that 
 * iterates in a loop over the different directories that 
 * constitute a File Store.
 * <p>
 * A new file can be created in the "current" directory (that is,
 * the directory last return by the {@link #next()}) via
 * the {@link #createFile(String)} method.
 * <p>
 * This iterator can also be provided with a collection of files.
 * The {@link #getFiles()} method returns a subset of these files
 * which belong to the current directory.
 *   
 * @author mvarshney
 *
 */
public class FileStoreIterator extends WeightedLoopIterator<File> {
	private static int fileCounter = 0;
	private final Multimap<String, File> fileMap = 
			new HashMultimap<String, File>();
	private static String Hostname;
	
	/**
	 * Creates a FileStoreIterator with the specified
	 * directories and respective weights.
	 * 
	 * @param dirs the specified directories
	 * @param weights respective weight of each directory
	 */
	public FileStoreIterator(File[] dirs, int[] weights) {
		super(dirs, weights);
		
		try {
			Hostname = java.net.InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException e) {
			Hostname = "localhost";
		}
	}
	
	/**
	 * Creates a FileStoreIterator with the specified
	 * directories and respective weights; additionally,
	 * specify the files in the FileStore.
	 * 
	 * @param dirs the specified directories
	 * @param weights respective weight of each directory
	 * @param files files belonging to the FileStore
	 * @throws IOException
	 */
	public FileStoreIterator(File[] dirs, int[] weights, 
			Collection<? extends File> files) throws IOException {
		super(dirs, weights);
		if (files != null)
			assignFiles(dirs, files);
		
		try {
			Hostname = java.net.InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException e) {
			Hostname = "localhost";
		}
	}
	
	/**
	 * Assigns files to the directories. The directories
	 * specify the different file systems, and this function
	 * maps each file to the filesystem it must belong to. On
	 * POSIX systems, the filesystem names may be descendents of
	 * others (for example, <tt>/</tt> and <tt>/mnt/disk2</tt>.
	 * 
	 * @param dirs the specified directories
	 * @param files the files that belong to one of these directories
	 * @throws IOException
	 */
	private void assignFiles(File[] dirs, Collection<? extends File> files) throws IOException {
		/* The strategy is as follows:
		 * 1. Create an array of the directory names
		 * 2. Sort the array in decreasing order of full directory
		 * 		name length.
		 * 3. Match each file against the above array to find
		 * 		the directory name that is a prefix of this filename.
		 * 4. The first match found is the correct directory to 
		 * 		which this file belongs.
		 */
		int len = dirs.length;
		String[] sortedDirs = new String[len];
		
		// Create the array of directory names
		for (int i = 0; i < len; i++) {
			// TODO: dirs[i] can be null here
			sortedDirs[i] = dirs[i].getCanonicalPath();
		}
		
		if (len > 1) {
			// Comparator to sort the array in decreasing order
			// of filename
			Comparator<String> comp = new Comparator<String>() {

				@Override
				public int compare(String o1, String o2) {
					return (int) Math.signum(o2.length() - o1.length());
				}
				
			};
			
			// Sort the array using the above comparator
			Arrays.sort(sortedDirs, comp);
			
			// Go through each file..
			for (File file: files) {
				if (file == null) continue;
				
				int i;
				// .. and find the first directory whose name
				// is prefix of this file
				for (i = 0; i < len; i++) {
					String fname = file.getCanonicalPath();
					if (fname.startsWith(sortedDirs[i])) {
						fileMap.put(sortedDirs[i], file);
						break;
					}
				}
				
				// When the file cannot be mapped to any directory
				if (i == len) {
					throw new IllegalArgumentException(file.getCanonicalPath() + 
							" cannot be assigned to any directory");
				}
			}
		} else {
			// If there is only one directory..
			fileMap.put(dirs[0].getCanonicalPath(), files);
		}
	}
	
	/**
	 * Creates a new (temporary) file in the current directory. The 
	 * file will be marked "deleteOnExit".
	 * 
	 * @param name prefix name of the file
	 * @return a newly created file
	 * @throws IOException
	 */
	public File createFile(String name) throws IOException {
		File root = last();
		File file = null;
		
		do {
			String filename = String.format("%s.%s.%04d", 
					Hostname, name, fileCounter);
			fileCounter++;
			file = new File(root, filename);
		} while (! file.createNewFile());
		
		//file.deleteOnExit();
		
		return file;
	}
	
	/**
	 * Returns a view of all the files in the current directory,
	 * or <tt>null</tt> if no files exist. The returned value
	 * is a view, so any change to the collection will be
	 * reflected in the subsequent calls.
	 * 
	 * @return View of all the file in this directory, or <tt>null</tt>
	 * if there are no files.
	 * @throws IOException
	 * @throws IllegalStateException if this method is called before
	 * calling {@link #next()} or after calling {@link #remove()}
	 */
	public Collection<File> getFiles() {
		String dir;
		try {
			dir = last().getCanonicalPath();
			return fileMap.getAll(dir);
		} catch (IOException e) {

		}
		return null;
	}
}
