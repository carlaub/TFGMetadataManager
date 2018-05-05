package utils;

import model.MMInformation;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

/**
 * Created by Carla Urrea Blázquez on 01/05/2018.
 *
 * ParserConf.java
 *
 * This class is a parser for the config file of the MetadataManager
 *
 * Actually, the config file must include this format:
 * 	 _______________________________________________________________________________________________
 * 	| 																								|
 * 	| Default FS																					|
 * 	| Working directory HDFS (Where files like "nodes information partition n" will be located)		|
 * 	| path (in HDFS) of the file that include the node's information								|
 * 	| path (in HDFS) of the file that include the edge's information								|
 * 	| Number of graph partitions 																	|
 * 	| Total number of SlaveNodes in the system														|
 * 	| IPs @ of slaves nodes																			|
 *	|_______________________________________________________________________________________________|
 */
public class ParserConf {
	BufferedReader brConfFile;

	public ParserConf() {

		try {
			brConfFile = new BufferedReader(new FileReader(System.getProperty("user.dir") + "/src/main/resources/files/MetadataManager.conf"));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	public MMInformation getConfiguration() throws IOException {
		MMInformation mmInformation = new MMInformation();
		int numSlavesNodes;
		String slaveIP;

		// Read default FS
		mmInformation.setDefaultFS(brConfFile.readLine());

		// Working directory HDFS
		mmInformation.setHDFSWorkingDirectory(brConfFile.readLine());

		// Read nodes information file in HDFS
		mmInformation.setHDFSPathNodesFile(brConfFile.readLine());

		// Read edged information file in HDFS
		mmInformation.setHDFSPathEdgesFile(brConfFile.readLine());

		// Read partitions number
		mmInformation.setNumberPartitions(Integer.valueOf(brConfFile.readLine()));

		// Read slaves nodes count
		numSlavesNodes = Integer.valueOf(brConfFile.readLine());
		mmInformation.setNumberSlaves(numSlavesNodes);

		// Read slaves nodes IP
		for (int i = 0; i < numSlavesNodes; i++) {
			mmInformation.addSlaveIP(brConfFile.readLine());
		}
		printConfigurationInformation(mmInformation);

		return mmInformation;
	}

	private void printConfigurationInformation(MMInformation mmInformation) {
		System.out.println("Nodes HDFS path: " + mmInformation.getHDFSPathNodesFile());
		System.out.println("Edges HDFS path: " + mmInformation.getHDFSPathEdgesFile());
		System.out.println("Slaves nodes number: " + mmInformation.getNumberSlaves());

		for (int i = 0; i < mmInformation.getNumberSlaves(); i++) {
			System.out.println("IP slave number " + (i + 1) + ": " + mmInformation.getSlaveIP(i + 1));
		}
	}
}
