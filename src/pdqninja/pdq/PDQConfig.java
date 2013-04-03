package pdqninja.pdq;

import java.io.Serializable;
import pdqninja.util.ByteUnit;

/**
 * Configurations for the PDQ environment. The current configuration
 * can be retrieved by the static {@link #current()} method.
 * <p>
 * <table border="1" style="border-collapse:collapse;padding=3;margin=3;">
 * <col width="15%"/>
 * <col width="15%"/>
 * <col width="15%"/>
 * <col width="55%"/>
 * <thead>
 * 	<tr style="background-color:lightgray;">
 * 		<th>Item</th>
 * 		<th>Command-line</th>
 * 		<th>Default</th>
 * 		<th>Description</th>
 *	</tr>
 *	</thead>
 *	<tbody>
 *	<tr>
 *		<td>Threads</td><td><code>-PDQ:Threads=&#35;</code></td><td>2</td>
 *		<td>Number of threads.</td>
 *	</tr>
 *
 *	<tr>
 *		<td>Local directory for temporary storage</td><td><code>-PDQ:LocalDir=&#35;</code></td><td><i>"."</i></td>
 *		<td>Local directory where data is externalized.</td>
 *	</tr>

 *	<tr>
 *		<td>Shared Directory</td><td><code>-PDQ:SharedDir=&#35;</code></td><td><i>empty string</i></td>
 *		<td>Shared directory that is accessible to all the PDQ workers in the distributed mode.</td>
 *	</tr>
 *
 *	<tr>
 *		<td>Blocksize</td><td><code>-PDQ:Blocksize=&#35;</code></td><td><i>64 MB</i></td>
 *		<td>Size of chunks that a (large) file is splitted into.</td>
 *	</tr>
 *
 *	<tr>
 *		<td>External File Index</td><td><code>-PDQ:Index=&#35;</code></td><td><i>8 KB</i></td>
 *		<td>
	 * Size of chunk for which one index entry is created. 
	 * <p>
	 * Indexes are created for external maps, that is, maps that are
	 * stored on disks. Note that the data is sorted (on map key) within
	 * the file. Each index entry is a pair of key and the offset
	 * within the file where this key appears. To reduce the memory
	 * required, the index entries are creates for a subset of the keys.
	 * This parameter indicates that one index entry will be created
	 * for the specified size. 
 *	</td>
 *	</tr>
 *
 *	<tr>
 *		<td>Buffer size</td><td><code>-PDQ:Buffer=&#35;</code></td><td><i>8 KB</i></td>
 *		<td>
	 * Buffer size of Buffered input and output streams.
	 * <p>
	 * PDQ read and writes data from external map files; this
	 * parameter configures the size of buffered streams.
 *	</td>
 *	</tr>
 *
 *	<tr>
 *		<td>ObjectOutputStream Reset</td><td><code>-PDQ:Reset=&#35;</code></td><td><i>128KB</i></td>
 *		<td>Amount of data after which the ObjectOutputStream will be reseted.</td>
 *	</tr>
 *
  *	<tr>
 *		<td>JVM Free Memory Threshold</td><td><code>-PDQ:MinFree=&#35;</code></td><td><i>4 GB</i></td>
 *		<td>
	 * Threshold of JVM free memory, below which the in-memory 
	 * data structures are externalized to disk.
 *	</td>
 *	</tr>
 *
 *	<tr>
 *		<td>Explicit Garbage Collection</td><td><code>-PDQ:Garbage=&#35;</code></td><td><i>false</i></td>
 *		<td>
	 * Explicitly perform garbage collection during the execution
	 * of PDQ.run().
	 * <p>
	 * If set to true, garbage collection will happen after 
	 * execution, after externalization, and after merge.
 *	</td>
 *	</tr>
 *
 *	<tr>
 *		<td>PDQ Workers</td><td><code>-PDQ:Workers=&#35;</code></td><td><i>empty string</i></td>
 *		<td>
	 * List of PDQ workers in distributed mode.
	 * <p>
	 * Each entry in the list is formatted as 
	 * "name-of-the-remote-host:port-number". Each entry within
	 * the list are separated by a comma.
 *	</td>
 *	</tr>
 *	</tbody>
 * </table>
 * 
 * <p>
 * <b>Listing Directories:</b> Certain items (Local Directory, Shared Directory and Disks)
 *  are configured with directory names. In a system
 * 
 * 
 * @author mvarshney
 *
 */
public class PDQConfig implements Serializable, Cloneable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -4637250079678173578L;

	/**
	 * Number of threads.
	 */
	private int threads = 2;
	
	/**
	 * Local directory where data is externalized. 
	 */
	private String localDir = ".";
	
	/**
	 * Shared directory that is accessible to all the 
	 * PDQ workers in the distributed mode.
	 */
	private String sharedDir = "";
	
	/**
	 *  
	 */
	private String disks = "";
	
	/**
	 * Size of chunks that a (large) file is splitted into.
	 */
	private long blocksize = ByteUnit.parse("32MB");
	
	/**
	 * Size of chunk for which one index entry is created. 
	 * <p>
	 * Indexes are created for external maps, that is, maps that are
	 * stored on disks. Note that the data is sorted (on map key) within
	 * the file. Each index entry is a pair of key and the offset
	 * within the file where this key appears. To reduce the memory
	 * required, the index entries are creates for a subset of the keys.
	 * This parameter indicates that one index entry will be created
	 * for the specified size. 
	 */
	private long externalIndex = ByteUnit.parse("8KB");
	
	/**
	 * Buffer size of Buffered input and output streams.
	 * <p>
	 * PDQ read and writes data from external map files; this
	 * parameter configures the size of buffered streams. 
	 */
	private long buffer = ByteUnit.parse("8KB");
	
	/**
	 * Amount of data after which the ObjectOutputStream will be reseted.
	 */
	private long reset = ByteUnit.parse("128KB");
	
	/**
	 * Threshold of JVM free memory, below which the in-memory 
	 * data structures are externalized to disk.
	 */
	private long minFree = ByteUnit.parse("4GB");
	
	/**
	 * Explicitly perform garbage collection during the execution
	 * of PDQ.run().
	 * <p>
	 * If set to true, garbage collection will happen after 
	 * execution, after externalization, and after merge.
	 */
	private boolean garbageCollect = false;
	
	/**
	 * List of PDQ workers in distributed mode.
	 * <p>
	 * Each entry in the list is formatted as 
	 * "<name of the remote host>:<port number>". Each entry within
	 * the list are separated by a comma.
	 */
	private String workers = "";
	
	private Boolean distribute = true;
	
	PDQConfig() {
		
	}
	
	private static PDQConfig current = new PDQConfig();
	
	public static PDQConfig current() {
		return current;
	}
	
	public static void setCurrent(PDQConfig conf) {
		current = conf;
	}
	
	/**
	 * Assign configuration values from command line arguments.
	 * @param args
	 */
	public void fromArgs(String[] args) {
		for (String arg: args) {
			if (! arg.startsWith("-PDQ:")) continue;
			String[] keyval = arg.split("=", 2);
			if (keyval.length != 2) continue;
			setOption(keyval[0].substring(5), keyval[1]);
		}
	}
	
	
	public void setOption(String key, String value) {
		if (key.equalsIgnoreCase("threads")) {
			setThreads((int) ByteUnit.parse(value));
		} else if (key.equalsIgnoreCase("localdir")) {
			setLocalDir(value);
		} else if (key.equalsIgnoreCase("shareddir")) {
			setSharedDir(value);
		} else if (key.equalsIgnoreCase("disks")) {
			setDisks(value);
		} else if (key.equalsIgnoreCase("blocksize")) {
			setBlocksize(ByteUnit.parse(value));
		} else if (key.equalsIgnoreCase("index")) {
			setExternalIndex(ByteUnit.parse(value));
		} else if (key.equalsIgnoreCase("buffer")) {
			setBuffer(ByteUnit.parse(value));
		} else if (key.equalsIgnoreCase("reset")) {
			setReset(ByteUnit.parse(value));
		} else if (key.equalsIgnoreCase("minfree")) {
			setMinFree(ByteUnit.parse(value));
		} else if (key.equalsIgnoreCase("garbage")) {
			setGarbageCollect(Boolean.parseBoolean(value));
		} else if (key.equalsIgnoreCase("workers")) {
			setWorkers(value);
		}
	}
	
	/**
	 * @return the number threads
	 */
	public int getThreads() {
		return threads;
	}

	/**
	 * @param threads the number threads to set
	 */
	public void setThreads(int threads) {
		this.threads = threads;
	}

	/**
	 * @return the localDir
	 */
	public String getLocalDir() {
		return localDir;
	}

	/**
	 * @param localDir the localDir to set
	 */
	public void setLocalDir(String localDir) {
		this.localDir = localDir;
	}

	/**
	 * @return the sharedDir
	 */
	public String getSharedDir() {
		return sharedDir;
	}

	/**
	 * @param sharedDir the sharedDir to set
	 */
	public void setSharedDir(String sharedDir) {
		this.sharedDir = sharedDir;
	}

	/**
	 * @return the blocksize
	 */
	public long getBlocksize() {
		return blocksize;
	}

	/**
	 * @param blocksize the blocksize to set
	 */
	public void setBlocksize(long blocksize) {
		this.blocksize = blocksize;
	}

	/**
	 * @return the externalIndex
	 */
	public long getExternalIndex() {
		return externalIndex;
	}

	/**
	 * @param externalIndex the externalIndex to set
	 */
	public void setExternalIndex(long externalIndex) {
		this.externalIndex = externalIndex;
	}

	/**
	 * @return the buffer
	 */
	public long getBuffer() {
		return buffer;
	}

	/**
	 * @param buffer the buffer to set
	 */
	public void setBuffer(long buffer) {
		this.buffer = buffer;
	}

	/**
	 * @return the reset
	 */
	public long getReset() {
		return reset;
	}

	/**
	 * @param reset the reset to set
	 */
	public void setReset(long reset) {
		this.reset = reset;
	}

	/**
	 * @return the minFree
	 */
	public long getMinFree() {
		return minFree;
	}

	/**
	 * @param minFree the minFree to set
	 */
	public void setMinFree(long minFree) {
		this.minFree = minFree;
	}

	/**
	 * @return the garbageCollect
	 */
	public boolean isGarbageCollect() {
		return garbageCollect;
	}

	/**
	 * @param garbageCollect the garbageCollect to set
	 */
	public void setGarbageCollect(boolean garbageCollect) {
		this.garbageCollect = garbageCollect;
	}

	/**
	 * @return the workers
	 */
	public String getWorkers() {
		return workers;
	}
	
	public int getNumWorkers() {
		if (workers == null || workers.equals(""))
			return 1;
		return workers.split(",").length + 1;
	}

	/**
	 * @param workers the workers to set
	 */
	public void setWorkers(String workers) {
		this.workers = workers;
	}

	/**
	 * @return the disks
	 */
	public String getDisks() {
		return disks;
	}

	/**
	 * @param disks the disks to set
	 */
	public void setDisks(String disks) {
		this.disks = disks;
	}

	
	public Boolean isDistributable() {
		return distribute;
	}
	
	@Override
	public PDQConfig clone() {
		try {
			return (PDQConfig) super.clone();
		} catch (CloneNotSupportedException e) {
			
		}
		return new PDQConfig();
	}

}
