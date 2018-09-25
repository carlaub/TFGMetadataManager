package hadoop;

import application.MetadataManager;
import constants.GenericConstants;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by Carla Urrea Blázquez on 26/04/2018.
 *
 * This class contains the methods and resources used for manage the Hadoop framework from the application.
 */
public class HadoopUtils {
	private static HadoopUtils instance;
	private FileSystem fs;

	private HadoopUtils() {
		configureHadoop();
	}

	/**
	 * Get the Hadoop instance.
	 * @return Hadoop instance.
	 */
	public static HadoopUtils getInstance() {
		if (instance == null) {
			instance = new HadoopUtils();
		}

		return instance;
	}

	/**
	 * Configure the Hadoops parameters according to the requirements of this project.
	 */
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
	 * Return the BufferReader of the HDFS file following the path *filePath*.
	 * @param filePath Complete file's path/url into HDFS with
	 *                 e.g. hdfs://node1-master:9000/user/hadoop/people.txt
	 * @return the BufferedReader.
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

	/**
	 * Update filed stored in Hadoop.
	 * @param fileName the name of the file name that will be updated.
	 * @param nodesToRemove list of all the nodes to be removed.
	 * @param nodesToUpdate list of all the nodes and properties to be updated.
	 * @param elementsToAdd list of the new elements to insert in the file.
	 */
	public List<Integer> updateGraphFile(String fileName, List<Integer> nodesToRemove, Map<Integer, Map<String, String>> nodesToUpdate, List<String> elementsToAdd) {
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
			int partsLenght;
			int nodeID;

			// Add existing nodes not removed
			while ((line = br.readLine()) != null) {
				parts = line.split("\\t");

				if (fileName.equals(GenericConstants.FILE_NAME_NODES)) {
					// NODE FILE
					int id = Integer.valueOf(parts[0]);

					if (!nodesToRemove.contains(id)) {

						// Check nodes to update
						partsLenght = parts.length;
						nodeID = Integer.valueOf(parts[0]);

						if (nodesToUpdate.containsKey(nodeID)) {

							int numLabels = Integer.valueOf(parts[1]);
							int i = numLabels + 2;

							while (i < partsLenght) {
								if (nodesToUpdate.get(nodeID).containsKey(parts[i])) {
									parts[i + 1] = nodesToUpdate.get(nodeID).get(parts[i]).replace("\"", "");

								}
								i = i + 2;
							}

							line = StringUtils.join(parts, '\t');
						}
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

	/**
	 * This function manage the resources to close when the system shuts down.
	 */
	public void closeResources() {
		try {
			if (fs != null) fs.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
