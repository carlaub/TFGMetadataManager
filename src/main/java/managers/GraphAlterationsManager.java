package managers;

import application.MetadataManager;
import constants.ErrorConstants;
import constants.GenericConstants;
import dataStructures.MapBorderNodes;
import queryStructure.QSNode;
import queryStructure.QSRelation;
import dataStructures.relationsTable.Relationship;
import dataStructures.relationsTable.RelationshipsTable;
import hadoop.HadoopUtils;

import java.io.*;
import java.util.*;

/**
 * Created by Carla Urrea Blázquez on 23/07/2018.
 *
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
	private Map<Integer, Map<String, String>> nodesToUpdate;


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
		nodesToUpdate = new HashMap<>();
	}

	/**
	 * Register and manage all the changes in the applications after insert a new relationship.
	 * @param qsNodeA QSNode origin.
	 * @param qsNodeB QSNode destination.
	 * @param qsRelation QSRelation to be insertes.
	 * @return Map (partition, queryToExecute)
	 */
	public Map<Integer, String> addNewRelation(QSNode qsNodeA, QSNode qsNodeB, QSRelation qsRelation) {
		Map<Integer, String> relCreationQueries = new HashMap<>();


		if (!(qsNodeA.getProperties().containsKey("id") && qsNodeA.getProperties().containsKey("id"))) return null;

		int idNodeA = Integer.valueOf(qsNodeA.getProperties().get("id"));
		int idNodeB = Integer.valueOf(qsNodeB.getProperties().get("id"));

		if (!mapGraphNodes.containsKey(idNodeA) || !mapGraphNodes.containsKey(idNodeB)) {
			System.out.println(ErrorConstants.ERR_RELATION_NODE_ID);
			return null;
		}

		int partitionNodeA = mapGraphNodes.get(idNodeA);
		int partitionNodeB = mapGraphNodes.get(idNodeB);


		// Save the relation to update the graph edge.txt file
		String relationGraphFilesFormat = qsRelation.toGraphFilesFormat(idNodeA, idNodeB);
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

			if (qsRelation.isRelationLTR()) {
				// (a)-->(b)
				relCreationQueries.put(partitionNodeA, "MATCH (a{id: " + idNodeA + "}), (x{id: " + borderIDPartA + "})" +
						"CREATE (a)" + qsRelation.toString() + "(x)");

				/* --------------------- */

				relCreationQueries.put(partitionNodeB, "MATCH (z{id: " + borderIDPartB + "}), (b{id: " + idNodeB + "})" +
						"CREATE (z)" + qsRelation.toString() + "(b)");

			} else {
				// (a)<--(b)
				relCreationQueries.put(partitionNodeB, "MATCH (b{id: " + idNodeB + "}), (z{id: " + borderIDPartB + "})" +
						"CREATE (b)" + qsRelation.toString() + "(z)");

				/* --------------------- */

				relCreationQueries.put(partitionNodeA, "MATCH (x{id: " + borderIDPartA + "}), (a{id: " + idNodeA + "})" +
						"CREATE (x)" + qsRelation.toString() + "(a)");
			}
		}

		return relCreationQueries;
	}

	/**
	 * Register and manage the insertion of a new node in the system.
	 * @param qsNode node to be inserted.
	 * @return the partition's ID where the node has been added.
	 */
	public int addNewNode(QSNode qsNode) {
		// Update NodeID --> PartitionID
		int partition = MetadataManager.getInstance().getLastPartitionFed();
		int nodeID = MetadataManager.getInstance().getMaxNodeId();

		qsNode.getProperties().put("id", String.valueOf(nodeID));
		mapGraphNodes.put(nodeID, partition);

		// Update graph files
		String nodeGraphFilesFormat = qsNode.toGraphFilesFormat();


		nodesToAdd.put(partition, nodeGraphFilesFormat);

		return partition;
	}

	/**
	 * Save the system's nodes updates.
	 * @param nodeID ID of the node that has been updated.
	 * @param property property to be updated.
	 * @param value new value of the property.
	 */
	public void recordUpdate(int nodeID, String property, String value) {
		if (!nodesToUpdate.containsKey(nodeID)) {
			Map<String, String> updates = new HashMap<>();
			nodesToUpdate.put(nodeID, updates);
		}

		nodesToUpdate.get(nodeID).put(property, value);
	}


	/**
	 * Register and manage the deletion of a node and relationships in the system.
	 * @param qsNode node to be deleted.
	 * @param originalQuery query without modification. This parameter is needed to be inserted into de list with the other detelete
	 *                      queries that must be executed if the node/relations are distributed between partitions.
	 * @return the queries that must to be executed in the systems.
	 */
	public Map<Integer, String> detachDeleteNode(QSNode qsNode, String originalQuery) {

		if (!qsNode.getProperties().containsKey("id")) {
			System.out.println(ErrorConstants.ERR_NODE_ID_DELETE);
			return null;
		}

		Map<Integer, String> deleteQueries = new HashMap<>();
		int nodeToRemoveID = Integer.valueOf(qsNode.getProperties().get("id"));
		int nodeToRemovePartition = mapGraphNodes.get(nodeToRemoveID);
		int borderNodeID;

		nodesToRemove.add(nodeToRemoveID);

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
						deleteQueries.put(iPart, "MATCH (n{id: " + borderNodeID + "})-[r]->(m{id: " + relation.getIdNodeDest() + "}) DELETE r");
					} else {
						deleteQueries.put(iPart, "MATCH (m{id: " + relation.getIdNodeOrg() + "})-[r]->(n{id: " + borderNodeID + "}) DELETE r");
					}
				}

				relationshipsTable.removeNodeRelations(borderNodeID);
			}

			// Remove relations from relationshipTable related with the partition of the removed node
			if (mapBorderNodes.contains(nodeToRemoveID, iPart)) {
				relationshipsTable.removeNodeRelations(nodeToRemoveID);

			}
		}

		return deleteQueries;
	}

	/**
	 * Check if exists a border node that connects two partitions.
	 * @param partOrg partition origin.
	 * @param partDes partition destination
	 * @param relCreationQueries set of queries that must to be executed to create the border node.
	 * @return the border's ID that connect the two partitions.
	 */
	private int checkBorderNode(int partOrg, int partDes, Map<Integer, String> relCreationQueries) {
		int borderNodeId;

		if (mapBorderNodes.contains(partOrg, partDes)) {
			// Border node exists
			return mapBorderNodes.getBorderNodeID(partOrg, partDes);
		} else {
			borderNodeId = MetadataManager.getInstance().getMaxNodeId();
			relCreationQueries.put(partOrg, "CREATE (n:border{id: " + borderNodeId + ",  partition: " + partDes + "})");

			mapBorderNodes.addNewBorderNode(partOrg, partDes, borderNodeId);

			return borderNodeId;
		}
	}

	/**
	 * Update the Hadoop node's file when the system shutdowns.
	 */
	private void updateNodesFiles() {
		Set<Map.Entry<Integer, String>> set = nodesToAdd.entrySet();
		List<Integer> numLinesRemoved;
		BufferedReader brMetis;
		BufferedWriter wMetisTemp;


		// General graph nodes file
		numLinesRemoved = HadoopUtils.getInstance().updateGraphFile(GenericConstants.FILE_NAME_NODES, nodesToRemove, nodesToUpdate, new ArrayList<>(nodesToAdd.values()));

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

		if (!tempFile.renameTo(new File(System.getProperty("user.dir") + GenericConstants.FILE_PATH_METIS))) {
			System.out.println(ErrorConstants.ERR_UPDATE_METIS_FILE);
		} else {
			System.out.println("--> METIS file updated\nBye!");
		}
	}

	/**
	 * Update the Hadoop edge's file when the system shutdowns.
	 */
	private void updateEdgesFiles() {
		// General graph nodes file
		HadoopUtils.getInstance().updateGraphFile(GenericConstants.FILE_NAME_EDGES, nodesToRemove, null, relationshipsToAdd);
	}

	/**
	 * Close resources and update the system's files.
	 */
	public void closeResources() {
		// Update files for future executions
		updateNodesFiles();
		updateEdgesFiles();
	}
}
