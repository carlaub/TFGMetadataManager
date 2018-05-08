package adapter;

import application.MetadataManager;
import constants.GenericConstants;
import utils.HadoopUtils;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * Created by Carla Urrea Blázquez on 22/04/2018.
 * MetisAdapter.java
 * <p>
 * Import the output file of METIS to the format defined and use by BatchInserter with the nodes information and edges.
 * This class need 3 main files to work properly:
 * -> Output file of METIS: each line corresponds to a number, the value indicates the partition where the node must be located.
 * -> Nodes file: file that contains, in txt format, all the nodes that form the graph
 * -> Edges file: file that contains, in txt format, all the relations between the nodes in the graph
 * <p>
 * The output expected after this process is: (Where n == number of graph partitions)
 * -> n nodes files, one per partition
 * -> n edges files, one per partition
 * <p>
 * maxNodeId is used to assign an id to the new BORDER NODES created to manage the relations broken
 * during the partition process
 */
public class MetisAdapter {

	private Map<Integer, Integer> mapGraphNodes;
	private Map<String, Integer> mapBorderNodes; // key = partition1 (1) cncat partition2 (2) = 12

	private String fileNameMetisOutput;

/*
	private List<BufferedWriter> bosNodes;
	private List<BufferedWriter> bosEdges;*/

	private List<BufferedOutputStream> bosNodes;
	private List<BufferedOutputStream> bosEdges;


	private int maxNodeId = 0;

	public MetisAdapter() {
		mapGraphNodes = new HashMap<Integer, Integer>();
		mapBorderNodes = new HashMap<String, Integer>();
	}

	public void beginExport(String fileNameMetisOutput, int partition) {
		this.fileNameMetisOutput = fileNameMetisOutput;

	/*	bosNodes = new ArrayList<BufferedWriter>();
		bosEdges = new ArrayList<BufferedWriter>();*/

		bosNodes = new ArrayList<BufferedOutputStream>();
		bosEdges = new ArrayList<BufferedOutputStream>();

		for (int i = 0; i < partition; i++) {
			try {
//				BufferedWriter bwNodesPart = new BufferedWriter(new FileWriter(GenericConstants.FILE_NAME_NODES_PARTITION_BASE + i + ".txt"));
//				BufferedWriter bwEdgesPart = new BufferedWriter(new FileWriter(GenericConstants.FILE_NAME_EDGES_PARTITION_BASE + i + ".txt"));
				BufferedOutputStream bwNodesPart = HadoopUtils.getInstance().createHDFSFile(GenericConstants.FILE_NAME_NODES_PARTITION_BASE + i + ".txt");
				BufferedOutputStream bwEdgesPart = HadoopUtils.getInstance().createHDFSFile(GenericConstants.FILE_NAME_EDGES_PARTITION_BASE + i + ".txt");

				bosNodes.add(bwNodesPart);
				bosEdges.add(bwEdgesPart);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		processNodeFile();
		processEdgeFile();

		closeResources();
	}

	/**
	 * Process the node files and distribute the information in sub-files, one for each partition
	 * based on METIS output
	 */
	private void processNodeFile() {
		BufferedReader readerMetisOutput;
		BufferedReader readerGraphNodes;
		String lineMetisOutput;
		String lineGraphNode;
		int nodeId;
		int partitionNumber;


		try {
			readerMetisOutput = new BufferedReader(new FileReader(fileNameMetisOutput));
			//readerGraphNodes = new BufferedReader(new FileReader(fileNameGraphNodes));
			readerGraphNodes = HadoopUtils.getInstance().getBufferReaderHFDSFile(MetadataManager.getInstance().getMMInformation().getHDFSPathNodesFile());


			while ((lineMetisOutput = readerMetisOutput.readLine()) != null) {
				lineGraphNode = readerGraphNodes.readLine();

				if (lineGraphNode != null) {
					nodeId = extractNodeId(lineGraphNode);
					partitionNumber = extractPartitionNumber(lineMetisOutput);

					mapGraphNodes.put(nodeId, partitionNumber);

					if (nodeId > maxNodeId) maxNodeId = nodeId;

					writeNodeInPartFile(lineGraphNode, partitionNumber);

					System.out.println("New node read: " + nodeId + " - " + partitionNumber);

				} else {
					//TODO: Show error format files no valid (#lines graph nodes == #lines metis-1)
				}
			}

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Process the edge file and distribute the information in sub-files, one for each partition
	 */
	private void processEdgeFile() {
		BufferedReader readerGraphEdges;
		String lineGraphEdges;
		String[] partsEgde;
		int idNodeFrom;
		int idNodeTo;
		int partitionFrom;
		int partitionTo;


		try {
			//readerGraphEdges = new BufferedReader(new FileReader(fileNameGraphEdges));
			readerGraphEdges = HadoopUtils.getInstance().getBufferReaderHFDSFile(MetadataManager.getInstance().getMMInformation().getHDFSPathEdgesFile());

			while ((lineGraphEdges = readerGraphEdges.readLine()) != null) {
				partsEgde = lineGraphEdges.split("\\t");
				idNodeFrom = Integer.valueOf(partsEgde[0]);
				idNodeTo = Integer.valueOf(partsEgde[1]);


				partitionFrom = mapGraphNodes.get(idNodeFrom);
				partitionTo = mapGraphNodes.get(idNodeTo);

				System.out.println("orig: " + idNodeFrom + "(" + partitionFrom + ") " + " dest: " + idNodeTo + "(" + partitionTo + ") ");

				if (partitionFrom == partitionTo) {
					// Nodes belong to the SAME partition, the relation hasn't been broken. Add the edge information
					// to the partition edges file
					writeEdgeInPartFile(lineGraphEdges, mapGraphNodes.get(idNodeFrom));
				} else {

					// The relation between nodes has been broken. We need to add the border node into nodes files of
					// of the two different partitions
					//TODO: Creation border nodes to control relations broken
					addRelationWithBoarderNode(partitionFrom, partitionTo, lineGraphEdges);

				}

			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}


	}

	private void addRelationWithBoarderNode(int partitionFrom, int partitionTo, String edgeInformation) {
		String keyBoarderMap;
		String[] partsEdgeInformation;
		Integer idBoarderNode;
		String newEdgeInformation;

		// Check if the first partition have or not a border node for the specific partition
		keyBoarderMap = String.valueOf(partitionFrom) + String.valueOf(partitionTo);
		idBoarderNode = mapBorderNodes.get(keyBoarderMap);

		if (idBoarderNode == null) {
			// There isn't any boarder node connecting these two partitions
			// Insert new node information into node files of the two partitions
			maxNodeId++;
			idBoarderNode = maxNodeId;
			System.out.println("Create border node.... " + idBoarderNode);

			writeNodeInPartFile(idBoarderNode + "	1	border	destination partition " + partitionTo, partitionFrom);
			mapBorderNodes.put(keyBoarderMap, idBoarderNode);
		}

		System.out.println("ID BOARDER " + idBoarderNode);

		partsEdgeInformation = edgeInformation.split("\\t");
		// We know that the second camp is the node desitnation ID. This node is replaced by the ID of the boarder node
		partsEdgeInformation[1] = String.valueOf(idBoarderNode);

		newEdgeInformation = "";
		for (String part : partsEdgeInformation) {
			newEdgeInformation = newEdgeInformation + part + "\t";
		}

		// Write the relation (between partition node and boarder node) in the edges file of the partition
		writeEdgeInPartFile(newEdgeInformation, partitionFrom);
	}

	private void writeNodeInPartFile(String lineGraphNode, int partition) {
		try {
//			bosNodes.get(partition).write(lineGraphNode + "\n");
			HadoopUtils.getInstance().writeHDFSFile(bosNodes.get(partition), lineGraphNode + "\n");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void writeEdgeInPartFile(String lineGraphEdege, int partition) {
		try {
//			bosEdges.get(partition).write(lineGraphEdege + "\n");
			HadoopUtils.getInstance().writeHDFSFile(bosEdges.get(partition), lineGraphEdege + "\n");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private int extractNodeId(String lineGraphNode) {
		String[] parts = lineGraphNode.split("\\t");
		return Integer.valueOf(parts[0]);
	}

	private int extractPartitionNumber(String lineMetisOutput) {
		return Integer.valueOf(lineMetisOutput);
	}

	private void closeResources() {

		for (BufferedOutputStream bw : bosNodes) {
			try {
				bw.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		for (BufferedOutputStream bw : bosEdges) {
			try {
				bw.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}
