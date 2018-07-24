package managers;

import application.MetadataManager;
import constants.ErrorConstants;
import constants.GenericConstants;
import queryStructure.QSNode;
import queryStructure.QSRelation;
import utils.HadoopUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Carla Urrea Blázquez on 23/07/2018.
 *
 * This Singleton class offers method to manager the graph alterations as insertions/deletions. Each one of those features requires
 * update files or generate subqueries.
 */
public class GraphAlterationsManager {
	private static GraphAlterationsManager instance;
	private Map<Integer, Integer> mapGraphNodes;
	private Map<String, Integer> mapBorderNodes;



	public static GraphAlterationsManager getInstance() {
		if (instance == null) instance = new GraphAlterationsManager();
		return instance;
	}

	private GraphAlterationsManager() {
		mapGraphNodes = MetadataManager.getInstance().getMapGraphNodes();
		mapBorderNodes = MetadataManager.getInstance().getMapBoarderNodes();
	}

	/**
	 *
	 * @param qsNodeA
	 * @param qsNodeB
	 * @param qsRelation
	 * @return Map (partiton, queryToExecute)
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

		// Write the original relation in the global edges.txt file
		String relationGraphFilesFormat = qsRelation.toGraphFilesFormat(idNodeA, idNodeB);
		HadoopUtils.getInstance().writeLineHDFSFile(GenericConstants.FILE_NAME_EDGES, relationGraphFilesFormat);


		if (partitionNodeA == partitionNodeB) {
			// Nodes are located in the same partition
			relCreationQueries.put(partitionNodeA, "MATCH (a{id: " + idNodeA + "}), (b{id: " + idNodeB + "}) " +
					"CREATE (a)" + qsRelation.toString() + "(b)");

			System.out.println("\n--> Send to p: " + partitionNodeA + "\n " + "MATCH (a{id: " + idNodeA + "}), (b{id: " + idNodeB + "}) " +
					"CREATE (a)" + qsRelation.toString() + "(b)");

			// Update the partition's edges file
			HadoopUtils.getInstance().writeLineHDFSFile(GenericConstants.FILE_NAME_EDGES_PARTITION_BASE + partitionNodeA+ ".txt", relationGraphFilesFormat);

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
				HadoopUtils.getInstance().writeLineHDFSFile(GenericConstants.FILE_NAME_EDGES_PARTITION_BASE + partitionNodeA + ".txt", relationGraphFilesFormat);


				/* --------------------- */

				relCreationQueries.put(partitionNodeB, "MATCH (z{id: " + borderIDPartB + "}), (b{id: " + idNodeB + "})" +
						"CREATE (z)" + qsRelation.toString() + "(b)");

				System.out.println("\n--> Send to p: " + partitionNodeB + "\n" + "MATCH (z{id: " + borderIDPartB + "}), (b{id: " + idNodeB + "})" +
						"CREATE (z)" + qsRelation.toString() + "(b)");

				relationGraphFilesFormat = qsRelation.toGraphFilesFormat(borderIDPartB, idNodeB);
				HadoopUtils.getInstance().writeLineHDFSFile(GenericConstants.FILE_NAME_EDGES_PARTITION_BASE + partitionNodeB + ".txt", relationGraphFilesFormat);


			} else {
				// (a)<--(b)
				relCreationQueries.put(partitionNodeB, "MATCH (b{id: " + idNodeB + "}), (z{id: " + borderIDPartB + "})" +
						"CREATE (b)" + qsRelation.toString() + "(z)");

				System.out.println("\n--> Send to p: " + partitionNodeB + "MATCH (b{id: " + idNodeB + "}), (z{id: " + borderIDPartB + "})" +
						"CREATE (b)" + qsRelation.toString() + "(z)");

				relationGraphFilesFormat = qsRelation.toGraphFilesFormat(idNodeB, borderIDPartB);
				HadoopUtils.getInstance().writeLineHDFSFile(GenericConstants.FILE_NAME_EDGES_PARTITION_BASE + partitionNodeB + ".txt", relationGraphFilesFormat);


				/* --------------------- */

				relCreationQueries.put(partitionNodeA, "MATCH (x{id: " + borderIDPartA + "}), (a{id: " + idNodeA + "})" +
						"CREATE (x)" + qsRelation.toString() + "(a)");

				System.out.println("\n--> Send to p: " + partitionNodeA + "MATCH (x{id: " + borderIDPartA + "}), (a{id: " + idNodeA + "})" +
						"CREATE (x)" + qsRelation.toString() + "(a)");

				relationGraphFilesFormat = qsRelation.toGraphFilesFormat(borderIDPartA, idNodeA);
				HadoopUtils.getInstance().writeLineHDFSFile(GenericConstants.FILE_NAME_EDGES_PARTITION_BASE + partitionNodeA + ".txt", relationGraphFilesFormat);
			}
		}

		return relCreationQueries;
	}



	private int checkBorderNode(int partOrg, int partDes, Map<Integer, String> relCreationQueries) {
		String key = String.valueOf(partOrg) + String.valueOf(partDes);
		int borderNodeId;

		if (!mapBorderNodes.containsKey(key)) {
			// Border node exists
			return mapBorderNodes.get(key);
		} else {
			borderNodeId = MetadataManager.getInstance().getMaxNodeId();
			relCreationQueries.put(partOrg, "CREATE (n:border{id: " + borderNodeId + ",  partition: " + partDes + "})");
			System.out.println("\n--> Send to p: " + partOrg + "\n " + "CREATE (n:border{id: " + borderNodeId + ",  partition: " + partDes + "})");

			mapBorderNodes.put(String.valueOf(partOrg)+String.valueOf(partDes), borderNodeId);

			return borderNodeId;
		}
	}

	public void addNewNode(QSNode qsNode) {
		// Update graph files
		String nodeGraphFilesFormat = qsNode.toGraphFilesFormat();

		HadoopUtils.getInstance().writeLineHDFSFile(GenericConstants.FILE_NAME_NODES_PARTITION_BASE + MetadataManager.getInstance().getLastPartitionFed() + ".txt", nodeGraphFilesFormat);
		HadoopUtils.getInstance().writeLineHDFSFile(GenericConstants.FILE_NAME_NODES, nodeGraphFilesFormat);
	}
}
