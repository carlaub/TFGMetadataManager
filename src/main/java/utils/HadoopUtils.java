package utils;

import application.MetadataManager;
import constants.GenericConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Carla Urrea Blázquez on 26/04/2018.
 *
 * HadoopUtils.java
 */
public class HadoopUtils {
	private static HadoopUtils instance;
	private FileSystem fs;

	public HadoopUtils() {
		configureHadoop();
	}

	public static HadoopUtils getInstance() {
		if (instance == null) {
			instance = new HadoopUtils();
		}

		return instance;
	}

	private void configureHadoop() {
		Configuration configuration = new Configuration();
		//Required by Maven
		configuration.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		configuration.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
		configuration.set("dfs.replication", "1");
		configuration.set("fs.defaultFS", MetadataManager.getInstance().getMMInformation().getDefaultFS());
		try {
			fs = FileSystem.get(configuration);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Return the BufferReader of the HDFS file following the path *filePath*
	 * @param filePath Complete file's path/url into HDFS with
	 *                 Ex format: hdfs://node1-master:9000/user/hadoop/people.txt
	 * @return
	 */
	public BufferedReader getBufferReaderHFDSFile(String filePath) {
		Path path = new Path(filePath);
		try {
			FSDataInputStream fsDataInputStream = fs.open(path);
			return new BufferedReader(new InputStreamReader(fsDataInputStream));

		} catch (IOException e) {
			e.printStackTrace();
		}

		return null;
	}

	/**
	 * Create new file in the working directory of HDFS. If the file exists, it is delete and re-create.
	 * @param fileName name of the file
	 * @return Buffered Output; "null" if the process was unsuccessful
	 * @throws IOException
	 */
	public BufferedOutputStream createHDFSFile(String fileName) throws IOException {
		Path path = new Path(MetadataManager.getInstance().getMMInformation().getHDFSWorkingDirectory() + fileName);
		if (fs.exists(path)) {
			fs.delete(path, true);
		}

		FSDataOutputStream fsout = fs.create(path);
		if (fsout == null) return null;

		return new BufferedOutputStream(fsout);
	}

	public void writeHDFSFile(BufferedOutputStream bos, String content) throws IOException {
		bos.write(content.getBytes("UTF-8"));
	}

//	public void writeHDFSFile() {
//		Path path = new Path("/user/hadoop/hola.txt");
//		try {
//			if (fs.exists(path)) {
//				fs.delete(path, true);
//			}
//			FSDataOutputStream fsout = fs.create(path);
//			BufferedOutputStream bos = new BufferedOutputStream(fsout);
//			bos.write("Works! :)".getBytes("UTF-8"));
//			bos.close();
//		} catch(IOException e) {
//			e.printStackTrace();
//		}
//	}

	public List<Integer> updateGraphFile(String fileName, List<Integer> nodesToRemove, List<String> elementsToAdd) {
		BufferedOutputStream bos;
		BufferedReader br;
		String line;
		int currentLine = 0;
		List<Integer> numLinesRemoved = new ArrayList<>();

		FSDataInputStream fsDataInputStream;
		Path orgPath = new Path(MetadataManager.getInstance().getMMInformation().getHDFSWorkingDirectory() + fileName);
		Path tempPath = new Path(MetadataManager.getInstance().getMMInformation().getHDFSWorkingDirectory() + fileName + ".tmp");

		try {
			FSDataOutputStream fsoutTemp = fs.create(tempPath);
			bos = new BufferedOutputStream(fsoutTemp);

			fsDataInputStream = fs.open(orgPath);
			br = new BufferedReader(new InputStreamReader(fsDataInputStream));

			String[] parts;

			// Add existing nodes not removed
			while ((line = br.readLine()) != null) {
				parts = line.split("\\t");

				if (fileName.equals(GenericConstants.FILE_NAME_NODES)) {
					// NODE FILE
					int id = Integer.valueOf(parts[0]);

					if (!nodesToRemove.contains(id)) {
						bos.write((line + "\n").getBytes("UTF-8"));
					} else {
						numLinesRemoved.add(currentLine);
					}
				} else if (fileName.equals(GenericConstants.FILE_NAME_EDGES)) {
					// RELATIONSHIP FILE
					int idOrg = Integer.valueOf(parts[0]);
					int idDest = Integer.valueOf(parts[1]);

					if (!nodesToRemove.contains(idOrg) && !nodesToRemove.contains(idDest)) {
						bos.write((line + "\n").getBytes("UTF-8"));
					}
				}

				currentLine ++;
			}

			// Add new nodes/relations
			for (String newNodeInfo : elementsToAdd) {
				bos.write((newNodeInfo + "\n").getBytes("UTF-8"));
			}

			// Remove original file
			fs.delete(orgPath, true);

			// Rename temp file
			fs.rename(tempPath, orgPath);

			bos.close();
			br.close();

			return numLinesRemoved;

		} catch (IOException e) {
			e.printStackTrace();
		}

		return null;
	}


	public void writeLineHDFSFile(String fileName, String content) {
		Path path = new Path("/user/hadoop/" + fileName);
		System.out.println("PATH: " + path.toString());

		try {
			FSDataOutputStream fsout = fs.append(path);
			PrintWriter pw = new PrintWriter(fsout);
			pw.append(content);
			pw.flush();
			fsout.hflush();
			pw.close();
			fsout.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void closeResources() {
		try {
			if (fs != null) fs.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
