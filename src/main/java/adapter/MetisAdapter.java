package adapter;

import application.MetadataManager;
import constants.GenericConstants;
import data.MapBorderNodes;
import relationsTable.Relationship;
import relationsTable.RelationshipsTable;
import utils.HadoopUtils;

import java.io.*;
import java.util.ArrayList;
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

	private Map<Integer, Integer> mapGraphNodes; // 1
	private MapBorderNodes mapBorderNodes; // 2 key = partition1 (1) cncat partition2 (2) = 12
	private RelationshipsTable relationshipsTable;

	private String fileNameMetisOutput;

/*	private List<BufferedWriter> bosNodes;
	private List<BufferedWriter> bosEdges;*/

	private List<BufferedOutputStream> bosNodes;
	private List<BufferedOutputStream> bosEdges;

	private int maxNodeId = 0;


	public MetisAdapter() {
		mapGraphNodes = MetadataManager.getInstance().getMapGraphNodes();
		mapBorderNodes = MetadataManager.getInstance().getMapBoarderNodes();
		relationshipsTable = MetadataManager.getInstance().getRelationshipsTable();

	}

	public void beginExport(String fileNameMetisOutput, int partition) {
		this.fileNameMetisOutput = fileNameMetisOutput;

/*		bosNodes = new ArrayList<BufferedWriter>();
		bosEdges = new ArrayList<BufferedWriter>();*/

		bosNodes = new ArrayList<BufferedOutputStream>();
		bosEdges = new ArrayList<BufferedOutputStream>();

		for (int i = 0; i < partition; i++) {
			try {
				/*BufferedWriter bwNodesPart = new BufferedWriter(new FileWriter(GenericConstants.FILE_NAME_NODES_PARTITION_BASE + i + ".txt"));
				BufferedWriter bwEdgesPart = new BufferedWriter(new FileWriter(GenericConstants.FILE_NAME_EDGES_PARTITION_BASE + i + ".txt"));
*/				BufferedOutputStream bwNodesPart = HadoopUtils.getInstance().createHDFSFile(GenericConstants.FILE_NAME_NODES_PARTITION_BASE + i + ".txt");
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

		// Update the last assigned ID for future node creations
		MetadataManager.getInstance().setMaxNodeId(maxNodeId);

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
//			readerGraphNodes = new BufferedReader(new FileReader("/Users/carlaurrea/Documents/Cuarto_Informatica/TFG/MetadataManager/src/main/resources/files/nodes.txt"));
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

			readerGraphNodes.close();
			readerMetisOutput.close();

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
//			readerGraphEdges = new BufferedReader(new FileReader("/Users/carlaurrea/Documents/Cuarto_Informatica/TFG/MetadataManager/src/main/resources/files/edges.txt"));
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

					addRelationWithBoarderNode(partitionFrom, partitionTo, lineGraphEdges, true);
//					addRelationWithBoarderNodeDestination(partitionFrom, partitionTo, lineGraphEdges);
					addRelationWithBoarderNode(partitionTo, partitionFrom, lineGraphEdges, false);
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 *
	 * @param localPartition
	 * @param foreignPartition
	 * @param edgeInformation
	 * @param relationDirection If the boarder node will be the origin or destination node [true N -> B] ; [false B -> N]
	 */
	private void addRelationWithBoarderNode(int localPartition, int foreignPartition, String edgeInformation, boolean relationDirection) {
		String keyBoarderMap;
		String[] partsEdgeInformation;
		Integer idBoarderNode;
		String newEdgeInformation;

		// Check if the first partition have or not a border node for the specific partition
		idBoarderNode = mapBorderNodes.getBorderNodeID(localPartition, foreignPartition);

		if (idBoarderNode == -1) {
			// There isn't any boarder node connecting these two partitions
			// Insert new node information into node files of the two partitions
			maxNodeId++;
			idBoarderNode = maxNodeId;
			System.out.println("Create border node.... " + idBoarderNode);

			writeNodeInPartFile(idBoarderNode + "	1	border	partition	" + foreignPartition, localPartition);
			mapBorderNodes.addNewBorderNode(localPartition, foreignPartition, idBoarderNode);

			mapGraphNodes.put(idBoarderNode, localPartition);
		}

		System.out.println("ID BORDER " + idBoarderNode);

		partsEdgeInformation = edgeInformation.split("\\t");

		String origNodeId = partsEdgeInformation[0];
		String destNodeId = partsEdgeInformation[1];

		// Insert the border node's relation in the relationshipsTable
		relationshipsTable.addRelation(idBoarderNode, new Relationship(Integer.parseInt(origNodeId), Integer.parseInt(destNodeId)));

		// Depending on the relation, the destination/origin node is replaced by the border node's id
		if (relationDirection) {
			partsEdgeInformation[1] = String.valueOf(idBoarderNode);
		} else {
			partsEdgeInformation[0] = String.valueOf(idBoarderNode);
		}

		newEdgeInformation = "";
		for (String part : partsEdgeInformation) {
			newEdgeInformation = newEdgeInformation + part + "\t";
		}
		// Add the ID of the original destination node located in a different partition
//		newEdgeInformation = newEdgeInformation + "idOriginalNode" + "\t" + destNodeId;

		// Write the relation (between partition node and boarder node) in the edges file of the partition
		writeEdgeInPartFile(newEdgeInformation, localPartition);
	}

/*	private void addRelationWithBoarderNodeDestination(int partitionFrom, int partitionTo, String edgeInformation) {
		String keyBoarderMap;
		String[] partsEdgeInformation;
		Integer idBoarderNode;
		String newEdgeInformation;

		// Check if the destination partition have or not a border node for the origin partition
		keyBoarderMap = String.valueOf(partitionTo) + String.valueOf(partitionFrom);
		idBoarderNode = mapBorderNodes.get(keyBoarderMap);

		if (idBoarderNode == null) {
			// There isn't any boarder node connecting these two partitions
			// Insert new node information into node files of the two partitions
			maxNodeId++;
			idBoarderNode = maxNodeId;
			System.out.println("Create border node.... " + idBoarderNode);

			writeNodeInPartFile(idBoarderNode + "	1	border	partition	" + partitionFrom, partitionTo);
			mapBorderNodes.put(keyBoarderMap, idBoarderNode);

			mapGraphNodes.put(nodeId, partitionNumber);

		}

		System.out.println("ID BOARDER " + idBoarderNode);

		partsEdgeInformation = edgeInformation.split("\\t");
		// We know that the second camp is the node destination ID. This node is replaced by the ID of the boarder node
		String originalNodeFromId = partsEdgeInformation[0];
		partsEdgeInformation[0] = String.valueOf(idBoarderNode);

		newEdgeInformation = "";
		for (String part : partsEdgeInformation) {
			newEdgeInformation = newEdgeInformation + part + "\t";
		}
		// Add the ID of the original destination node located in a different partition
		newEdgeInformation = newEdgeInformation + "idOriginalNode" + "\t" + originalNodeFromId;

		// Write the relation (between partition node and boarder node) in the edges file of the partition
		writeEdgeInPartFile(newEdgeInformation, partitionTo);
	}*/

	private void writeNodeInPartFile(String lineGraphNode, int partition) {
		try {
			HadoopUtils.getInstance().writeHDFSFile(bosNodes.get(partition), lineGraphNode + "\n");
//			bosNodes.get(partition).write(lineGraphNode + "\n");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void writeEdgeInPartFile(String lineGraphEdege, int partition) {
		try {
			HadoopUtils.getInstance().writeHDFSFile(bosEdges.get(partition), lineGraphEdege + "\n");
//			bosEdges.get(partition).write(lineGraphEdege + "\n");

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
/*
		for ( BufferedWriter bw : bosNodes) {
			try {
				bw.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		for (BufferedWriter bw : bosEdges) {
			try {
				bw.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}*/
	}
}
