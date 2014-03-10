package graphBuilder;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;

public class FileGetter {

	static ArrayList<File> files;
	static {
		files = new ArrayList<File>(Arrays.asList(new File(".").listFiles()));
	}

	static ArrayList<File> getListForPrefix(String prefix, boolean TextOnly) {
		ArrayList<File> res = new ArrayList<File>();
		for (File file : files) {
			if (file.getName().startsWith(prefix)
					&& (!TextOnly || file.getName().endsWith(".txt")))
				res.add(file);
		}
		return res;
	}
}
