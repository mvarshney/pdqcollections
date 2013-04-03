package pdqninja.pdqio;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import pdqninja.pdq.PDQConfig;

public class FileStores {
	private static Pattern pattern = 
			Pattern.compile("^\\s*(.*):(\\d+)\\s*$");

	static FileStoreIterator getStorage(String dirList, 
			Collection<? extends File> files) throws IOException {
		File[] dirs;
		int[] weights;
		
		String[] dirNames = dirList.split(",");
		int ndirs = dirNames.length;
		 
		dirs = new File[ndirs];
		weights = new int[ndirs];
		
		for (int i = 0; i < ndirs; i++) {
			String name = dirNames[i];
			File f = new File(name);
			if (! f.exists()) {
				Matcher m = pattern.matcher(name);
				if (m.matches()) {
					try {
						f = new File(m.group(1));
						int weight = Integer.parseInt(m.group(2));
						
						if (!f.exists())
							throw new FileNotFoundException(m.group(1));
						
						dirs[i] = f;
						weights[i] = weight;
					} catch (NumberFormatException e) {
						throw new FileNotFoundException(name);
					}
				}
			} else {
				dirs[i] = f;
				weights[i] = 1;
			}
		}

		return new FileStoreIterator(dirs, weights, files);
	}
	
	public static FileStoreIterator getLocalStorage() throws IOException {
		String dir = PDQConfig.current().getLocalDir().trim();
		if (dir == null || dir.equals(""))
			return null;
		return getStorage(dir, null);
	}
	
	public static FileStoreIterator getSharedStorage() throws IOException {
		String dir = PDQConfig.current().getSharedDir().trim();
		if (dir == null || dir.equals(""))
			return null;
		return getStorage(dir, null);
	}
	
	public static FileStoreIterator getDiskStorage(Collection<? extends File> files) throws IOException {
		String disks = PDQConfig.current().getDisks();
		// TODO: works on linux only
		if (disks == null || disks.equals(""))
			disks = "/";
		return getStorage(disks, files);
	}
}
