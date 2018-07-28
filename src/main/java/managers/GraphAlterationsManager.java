package managers;

import application.MetadataManager;
import constants.ErrorConstants;
import constants.GenericConstants;
import data.MapBorderNodes;
import queryStructure.QSNode;
import queryStructure.QSRelation;
import relationsTable.Relationship;
import relationsTable.RelationshipsTable;
import utils.HadoopUtils;

import java.io.*;
import java.util.*;

/**
 * Created by Carla Urrea Blázquez on 23/07/2018.
 * <p>
 * This Singleton class offers method to manager the graph alterations as insertions/deletions. Each one of those features requires
 * update files or generate subqueries.
 */
public class GraphAlterationsManager {
	private static GraphAlterationsManager instance;
	private Map<Integer, Integer> mapGraphNodes;
	private MapBorderNodes mapBorderNodes;

	private List<Integer> nodesToRemove;
	private List<String> relationshipsToAdd;
	private Map<Integer, String> nodesToAdd;


	public static GraphAlterationsManager getInstance() {
		if (instance == null) instance = new GraphAlterationsManager();
		return instance;
	}

	private GraphAlterationsManager() {
		mapGraphNodes = MetadataManager.getInstance().getMapGraphNodes();
		mapBorderNodes = MetadataManager.getInstance().getMapBoarderNodes();
		nodesToRemove = new ArrayList<>();
		relationshipsToAdd = new ArrayList<>();
		nodesToAdd = new HashMap<>();

	}

	/**
	 * @param qsNodeA
	 * @param qsNodeB
	 * @param qsRelation
	 * @return Map (partiton, queryToExecute)
	 */
	public Map<Integer, String> addNewRelation(QSNode qsNodeA, QSNode qsNodeB, QSRelation qsRelation) {
		Map<Integer, String> relCreationQueries = new HashMap<>();

		System.out.println("Add new relation");

		if (!(qsNodeA.getProperties().containsKey("id") && qsNodeA.getProperties().containsKey("id"))) return null;

		int idNodeA = Integer.valueOf(qsNodeA.getProperties().get("id"));
		int idNodeB = Integer.valueOf(qsNodeB.getProperties().get("id"));

		if (!mapGraphNodes.containsKey(idNodeA) || !mapGraphNodes.containsKey(idNodeB)) {
			System.out.println(ErrorConstants.ERR_RELATION_NODE_ID);
			return null;
		}

		int partitionNodeA = mapGraphNodes.get(idNodeA);
		int partitionNodeB = mapGraphNodes.get(idNodeB);

		System.out.println("--> NodeA: " + idNodeA + " part: " + partitionNodeA + "\n    NodeB: " + idNodeB + " part: " + partitionNodeB);

		// Save the relation to update the graph edge.txt file
		String relationGraphFilesFormat = qsRelation.toGraphFilesFormat(idNodeA, idNodeB);
//		HadoopUtils.getInstance().writeLineHDFSFile(GenericConstants.FILE_NAME_EDGES, relationGraphFilesFormat);
		relationshipsToAdd.add(relationGraphFilesFormat);


		if (partitionNodeA == partitionNodeB) {
			// Nodes are located in the same partition
			relCreationQueries.put(partitionNodeA, "MATCH (a{id: " + idNodeA + "}), (b{id: " + idNodeB + "}) " +
					"CREATE (a)" + qsRelation.toString() + "(b)");

			System.out.println("\n--> Send to p: " + partitionNodeA + "\n " + "MATCH (a{id: " + idNodeA + "}), (b{id: " + idNodeB + "}) " +
					"CREATE (a)" + qsRelation.toString() + "(b)");


		} else {
			// Nodes are located in different partitions

			int borderIDPartA = checkBorderNode(partitionNodeA, partitionNodeB, relCreationQueries);
			int borderIDPartB = checkBorderNode(partitionNodeB, partitionNodeA, relCreationQueries);

			System.out.println("--> BorderIDPartA: " + borderIDPartA);
			System.out.println("--> BorderIDPartB: " + borderIDPartB);


			if (qsRelation.isRelationLTR()) {
				// (a)-->(b)
				relCreationQueries.put(partitionNodeA, "MATCH (a{id: " + idNodeA + "}), (x{id: " + borderIDPartA + "})" +
						"CREATE (a)" + qsRelation.toString() + "(x)");

				System.out.println("\n--> Send to p: " + partitionNodeA + "\n" + "MATCH (a{id: " + idNodeA + "}), (x{id: " + borderIDPartA + "})" +
						"CREATE (a)" + qsRelation.toString() + "(x)");

				relationGraphFilesFormat = qsRelation.toGraphFilesFormat(idNodeA, borderIDPartA);
//				HadoopUtils.getInstance().writeLineHDFSFile(GenericConstants.FILE_NAME_EDGES_PARTITION_BASE + partitionNodeA + ".txt", relationGraphFilesFormat);


				/* --------------------- */

				relCreationQueries.put(partitionNodeB, "MATCH (z{id: " + borderIDPartB + "}), (b{id: " + idNodeB + "})" +
						"CREATE (z)" + qsRelation.toString() + "(b)");

				System.out.println("\n--> Send to p: " + partitionNodeB + "\n" + "MATCH (z{id: " + borderIDPartB + "}), (b{id: " + idNodeB + "})" +
						"CREATE (z)" + qsRelation.toString() + "(b)");

				relationGraphFilesFormat = qsRelation.toGraphFilesFormat(borderIDPartB, idNodeB);
//				HadoopUtils.getInstance().writeLineHDFSFile(GenericConstants.FILE_NAME_EDGES_PARTITION_BASE + partitionNodeB + ".txt", relationGraphFilesFormat);


			} else {
				// (a)<--(b)
				relCreationQueries.put(partitionNodeB, "MATCH (b{id: " + idNodeB + "}), (z{id: " + borderIDPartB + "})" +
						"CREATE (b)" + qsRelation.toString() + "(z)");

				System.out.println("\n--> Send to p: " + partitionNodeB + "MATCH (b{id: " + idNodeB + "}), (z{id: " + borderIDPartB + "})" +
						"CREATE (b)" + qsRelation.toString() + "(z)");

				relationGraphFilesFormat = qsRelation.toGraphFilesFormat(idNodeB, borderIDPartB);
//				HadoopUtils.getInstance().writeLineHDFSFile(GenericConstants.FILE_NAME_EDGES_PARTITION_BASE + partitionNodeB + ".txt", relationGraphFilesFormat);


				/* --------------------- */

				relCreationQueries.put(partitionNodeA, "MATCH (x{id: " + borderIDPartA + "}), (a{id: " + idNodeA + "})" +
						"CREATE (x)" + qsRelation.toString() + "(a)");

				System.out.println("\n--> Send to p: " + partitionNodeA + "MATCH (x{id: " + borderIDPartA + "}), (a{id: " + idNodeA + "})" +
						"CREATE (x)" + qsRelation.toString() + "(a)");

				relationGraphFilesFormat = qsRelation.toGraphFilesFormat(borderIDPartA, idNodeA);
//				HadoopUtils.getInstance().writeLineHDFSFile(GenericConstants.FILE_NAME_EDGES_PARTITION_BASE + partitionNodeA + ".txt", relationGraphFilesFormat);
			}
		}

		return relCreationQueries;
	}


	public int addNewNode(QSNode qsNode) {
		// Update NodeID --> PartitionID
		int partition = MetadataManager.getInstance().getLastPartitionFed();
		int nodeID = MetadataManager.getInstance().getMaxNodeId();

		qsNode.getProperties().put("id", String.valueOf(nodeID));
		mapGraphNodes.put(nodeID, partition);

		// Update graph files
		String nodeGraphFilesFormat = qsNode.toGraphFilesFormat();

//		HadoopUtils.getInstance().writeLineHDFSFile(GenericConstants.FILE_NAME_NODES_PARTITION_BASE + partition + ".txt", nodeGraphFilesFormat);

		nodesToAdd.put(partition, nodeGraphFilesFormat);
//		HadoopUtils.getInstance().writeLineHDFSFile(GenericConstants.FILE_NAME_NODES, nodeGraphFilesFormat);

		return partition;
	}


	public Map<Integer, String> detachDeleteNode(QSNode qsNode, String originalQuery) {

		if (!qsNode.getProperties().containsKey("id")) {
			System.out.println(ErrorConstants.ERR_NODE_ID_DELETE);
			return null;
		}

		Map<Integer, String> deleteQueries = new HashMap<>();
		int nodeToRemoveID = Integer.valueOf(qsNode.getProperties().get("id"));
		int nodeToRemovePartition = mapGraphNodes.get(nodeToRemoveID);
		int borderNodeID;

		// Add the original query (MATCH DETACH [...]) to the deleteQueries structure that store the sub-queries required to delete the node's relations
		// in the rest of partitions
		deleteQueries.put(nodeToRemovePartition, originalQuery);

		// For each partition's border node, check if there is any relation with an foreign node. If there is, add the sub-query
		// required and remove it from the [relationshipsTable] global structure.
		RelationshipsTable relationshipsTable = MetadataManager.getInstance().getRelationshipsTable();
		int partitionsNum = MetadataManager.getInstance().getMMInformation().getNumberPartitions();

		for (int iPart = 0; iPart < partitionsNum; iPart++) {
			if (nodeToRemovePartition == iPart) continue;

			if (mapBorderNodes.contains(iPart, nodeToRemovePartition)) {
				borderNodeID = mapBorderNodes.getBorderNodeID(iPart, nodeToRemovePartition);

				List<Relationship> relationsremovedNode = relationshipsTable.getNodeRelationships(borderNodeID, nodeToRemoveID);

				for (Relationship relation : relationsremovedNode) {

					if (relation.getIdNodeOrg() == nodeToRemoveID) {
						deleteQueries.put(iPart, "MATCH (n{id: " + borderNodeID + "})-[r]->(m{id: " + relation.getIdNodeDest() + ") DELETE r");
					} else {
						deleteQueries.put(iPart, "MATCH (m{id: " + relation.getIdNodeOrg() + "})-[r]->(n{id: " + borderNodeID + ") DELETE r");
					}
				}

				relationshipsTable.removeNodeRelations(borderNodeID, nodeToRemoveID);
			}

			// Remove relations from relationshipTable related with the partition of the removed node
			if (mapBorderNodes.contains(nodeToRemoveID, iPart)) {
				borderNodeID = mapBorderNodes.getBorderNodeID(iPart, nodeToRemovePartition);
				relationshipsTable.removeNodeRelations(nodeToRemoveID, borderNodeID);

			}
		}


		// TODO: update relationshipTable border node local


		return deleteQueries;
	}


	private int checkBorderNode(int partOrg, int partDes, Map<Integer, String> relCreationQueries) {
		int borderNodeId;

		if (mapBorderNodes.contains(partOrg, partDes)) {
			// Border node exists
			return mapBorderNodes.getBorderNodeID(partOrg, partDes);
		} else {
			borderNodeId = MetadataManager.getInstance().getMaxNodeId();
			relCreationQueries.put(partOrg, "CREATE (n:border{id: " + borderNodeId + ",  partition: " + partDes + "})");
			System.out.println("\n--> Send to p: " + partOrg + "\n " + "CREATE (n:border{id: " + borderNodeId + ",  partition: " + partDes + "})");

			mapBorderNodes.addNewBorderNode(partOrg, partDes, borderNodeId);

			return borderNodeId;
		}
	}


	private void updateNodesFiles() {
		Set<Map.Entry<Integer, String>> set = nodesToAdd.entrySet();
		List<Integer> numLinesRemoved;
		BufferedReader brMetis;
		BufferedWriter wMetisTemp;


		// General graph nodes file
		numLinesRemoved = HadoopUtils.getInstance().updateGraphFile(GenericConstants.FILE_NAME_NODES, nodesToRemove, new ArrayList<>(nodesToAdd.values()));

		// Metis output file

		File tempFile = new File("metisTemp.txt");
		String currentLine;
		int numCurrentLine = 0;

		try {
			wMetisTemp = new BufferedWriter(new FileWriter(tempFile));
			brMetis = new BufferedReader(new FileReader(System.getProperty("user.dir") + GenericConstants.FILE_PATH_METIS));

			while ((currentLine = brMetis.readLine()) != null) {

				if (numLinesRemoved.contains(numCurrentLine)) continue;
				wMetisTemp.write(currentLine + "\n");
			}

			// Add new nodes
			for (Map.Entry entry : set) {
				wMetisTemp.write(entry.getKey() + "\n");
			}

			wMetisTemp.close();
			brMetis.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		if (!tempFile.renameTo(new File(System.getProperty("user.dir") + GenericConstants.FILE_NAME_METIS))) {
			System.out.println(ErrorConstants.ERR_UPDATE_METIS_FILE);
		} else {
			System.out.println("--> Metis file updated");
		}
	}

	public void updateEdgesFiles() {
		// General graph nodes file
		HadoopUtils.getInstance().updateGraphFile(GenericConstants.FILE_NAME_EDGES, nodesToRemove, relationshipsToAdd);
	}

	public void closeResources() {
		// Update files for future executions
		updateNodesFiles();
		updateEdgesFiles();
	}
}
