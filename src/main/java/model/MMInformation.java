package model;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Carla Urrea Blázquez on 01/05/2018.
 *
 * MMInformation.java
 *
 * Class model that defines the MetadataManager basic information. This information
 * is needed in different processes like: establish communication with slaves nodes,
 * read graph information from Hadoop files etc.
 */
public class MMInformation {
	private String defaultFS;
	private String HDFSWorkingDirectory;
	private String HDFSPathNodesFile;
	private String HDFSPathEdgesFile;
	private int numberPartitions;
	private int numberSlaves;
	private List<String> slavesIP;

	public MMInformation() {
		slavesIP = new ArrayList<String>();
	}

	public String getHDFSPathNodesFile() {
		return HDFSPathNodesFile;
	}

	public void setHDFSPathNodesFile(String HDFSPathNodesFile) {
		this.HDFSPathNodesFile = HDFSPathNodesFile;
	}

	public String getHDFSPathEdgesFile() {
		return HDFSPathEdgesFile;
	}

	public void setHDFSPathEdgesFile(String HDFSPathEdgesFile) {
		this.HDFSPathEdgesFile = HDFSPathEdgesFile;
	}

	public int getNumberSlaves() {
		return numberSlaves;
	}

	public void setNumberSlaves(int numberSlaves) {
		this.numberSlaves = numberSlaves;
	}

	public void addSlaveIP(String IP) {
		if (slavesIP != null) slavesIP.add(IP);
	}

	public String getDefaultFS() {
		return defaultFS;
	}

	public void setDefaultFS(String defaultFS) {
		this.defaultFS = defaultFS;
	}

	public String getHDFSWorkingDirectory() {
		return HDFSWorkingDirectory;
	}

	public void setHDFSWorkingDirectory(String HDFSWorkingDirectory) {
		this.HDFSWorkingDirectory = HDFSWorkingDirectory;
	}

	public int getNumberPartitions() {
		return numberPartitions;
	}

	public void setNumberPartitions(int numberPartitions) {
		this.numberPartitions = numberPartitions;
	}

	/**
	 * Get the IP for the node slave *slaveNum*. Slave number starts in 1 !
	 * Ex: get IP for the node slave 1 -> get from the IP's list the item 0
	 *
	 * @param slaveNum Slave node identification [1, 2 ...]
	 */
	public String getSlaveIP(int slaveNum) {
		if (slavesIP == null || slaveNum < 0 || slaveNum > slavesIP.size()) return null;
		return slavesIP.get(slaveNum - 1);
	}
}
