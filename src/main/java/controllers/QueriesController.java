package controllers;

import application.MetadataManager;
import constants.ErrorConstants;
import constants.GenericConstants;
import dataStructures.MapBorderNodes;
import dnl.utils.text.table.TextTable;
import managers.GraphAlterationsManager;
import neo4j.*;
import network.MMServer;
import parser.Type;
import queryStructure.*;
import dataStructures.relationsTable.RelationshipsTable;

import java.util.*;

/**
 * Created by Carla Urrea Blázquez on 06/07/2018.
 * <p>
 * This controller manage the execution and the results of a query.
 */
public class QueriesController {
	private MMServer mmServer;
	private QueryExecutor queryExecutor;
	private ResultQuery initialResultQuery;
	private ResultNode rootNode;
	private RelationshipsTable relationshipsTable;
	private List<String> exploredBorderNodes;
	private GraphAlterationsManager gam;
	private QueryStructure originalQueryStructure;
	private int broadcastsReceived;

	private int explorationWithResults;
	private int chainedLastNodeId;


	public QueriesController() {
		mmServer = MMServer.getInstance();
		queryExecutor = new QueryExecutor();
		initialResultQuery = null;
		rootNode = null;
		relationshipsTable = MetadataManager.getInstance().getRelationshipsTable();
		exploredBorderNodes = new ArrayList<>();
		gam = GraphAlterationsManager.getInstance();

	}

	/**
	 * Send query to the partition that hosts the root node of the query.
	 * @param queryStructure queryStructure to be send.
	 * @param idRootNode ID of the root node. This is useful for obtain the partition where it is located and execute the query
	 *                   on it.
	 */
	private void sendById(QueryStructure queryStructure, int idRootNode) {
		sendById(queryStructure, idRootNode, false);
	}

	private void sendById(QueryStructure queryStructure, int idRootNode, boolean trackingMode) {
		int partitionID = MetadataManager.getInstance().getMapGraphNodes().get(idRootNode);
		sendByPartitionID(queryStructure, partitionID, trackingMode);
	}

	/**
	 * Send the query to the partition with [partitionID].
	 * @param queryStructure QueryStructure to be send.
	 * @param partitionID ID of the partition.
	 */
	private void sendByPartitionID(QueryStructure queryStructure, int partitionID) {
		sendByPartitionID(queryStructure, partitionID, false);
	}

	private void sendByPartitionID(QueryStructure queryStructure, int partitionID, boolean trackingMode) {
		if (partitionID == GenericConstants.MM_SLAVE_NODE_ID) {
			// Root node is inside MetadataManager's Neo4j instance
			queryExecutor.processQuery(queryStructure, this, trackingMode);
		} else {
			// Root node is in the slave node with id "partitionID"
			mmServer.sendQuery(partitionID, queryStructure, this, trackingMode);
		}
	}

	/**
	 * Manage the execution of a new query.
	 * @param queryStructure QueryStructure to be analysed.
	 */
	public void manageNewQuery(QueryStructure queryStructure) {
		int queryType = queryStructure.getQueryType();
		int idRootNode;

		originalQueryStructure = queryStructure;
		System.out.println("-> Query: \n" + originalQueryStructure);

		switch (queryType) {
			case QueryStructure.QUERY_TYPE_CREATE:
				manageQueryCreate(queryStructure);
				break;

			case QueryStructure.QUERY_TYPE_DETACH:
				manageQueryDelete(queryStructure);
				break;

			case QueryStructure.QUERY_TYPE_CHAINED:
				idRootNode = queryStructure.getRootNodeId();
				int matchVarsCount = queryStructure.getMatchVariablesCount();

				// Derived Sub-queries
				for (int i = (matchVarsCount - 1); i > 0; i--) {
					chainedLastNodeId = originalQueryStructure.getRootNodeId();
					sendById(queryStructure.getSubChainQuery(0, i, -1, -1), idRootNode);
				}

				// Send Original query
				sendById(queryStructure, idRootNode, false);

				break;

			case QueryStructure.QUERY_TYPE_UPDATE:
				manageQuerySet(queryStructure);
				break;

			default:
				// CASE 1: Query's MATCH clause has a relation
				idRootNode = queryStructure.getRootNodeId();


				if (idRootNode > 0) {
					sendById(queryStructure, idRootNode);
				} else {
					// CASE 2: Query's MATCH clause has not a relation
					broadcastsReceived = 0;

					queryStructure.setQueryType(QueryStructure.QUERY_TYPE_BROADCAST);
					mmServer.sendQueryBroadcast(queryStructure, this);
					queryExecutor.processQuery(queryStructure, this, false);
				}

				break;
		}
	}

	/**
	 * Manage queries of the CREATE type.
	 * @param queryStructure QueryStructure to be analysed.
	 */
	private void manageQueryCreate(QueryStructure queryStructure) {
		int partition;

		if (queryStructure.hasRelation()) {

			// CREATE RELATION
			// The nodes of the relationship with their IDs are recovered
			QSNode qsNodeA = queryStructure.getMatchNodeAt(0);
			QSNode qsNodeB = queryStructure.getMatchNodeAt(1);
			QSRelation qsRelation = queryStructure.getCreateRelation();

			if (qsNodeA != null && qsNodeB != null && qsRelation != null) {
				Map<Integer, String> relCreationQueries;
				Set<Map.Entry<Integer, String>> set;
				relCreationQueries = gam.addNewRelation(qsNodeA, qsNodeB, qsRelation);

				// If the relation is established between node located in different partitions, some subqueries must
				// be executed to create relations with border nodes or insert new border nodes if are required.
				set = relCreationQueries.entrySet();
				for (Map.Entry<Integer, String> entry : set) {
					partition = entry.getKey();
					if (partition == 0) {
						// Local Neo4j DB
						queryExecutor.processQuery(entry.getValue());
					} else {
						mmServer.sendStringQuery(partition, entry.getValue());
					}
				}

			} else {
				System.out.println(ErrorConstants.ERR_RELATION_CREATION);
			}
		} else {
			// CREATE NODE
			int partitionID = gam.addNewNode(queryStructure.getRootNode());
			System.out.println("\nQuery: " + queryStructure.toString() + "\n");
			sendByPartitionID(queryStructure, partitionID);
		}
	}

	/**
	 * Manage queries of the DELETE type.
	 * @param queryStructure QueryStructure to be analysed.
	 */
	private void manageQueryDelete(QueryStructure queryStructure) {
		Map<Integer, String> queriesDelete = gam.detachDeleteNode(queryStructure.getRootNode(), queryStructure.toString());
		Set<Map.Entry<Integer, String>> set = queriesDelete.entrySet();
		int partition;

		for (Map.Entry<Integer, String> entry : set) {
			partition = entry.getKey();
			if (partition == 0) {
				// Local Neo4j DB
				queryExecutor.processQuery(entry.getValue());
			} else {
				mmServer.sendStringQuery(partition, entry.getValue());
			}
		}
	}

	/**
	 * Manage queries of the SET type.
	 * @param queryStructure QueryStructure to be analysed.
	 */
	private void manageQuerySet(QueryStructure queryStructure) {
		int idRootNode = queryStructure.getRootNodeId();
		QSSet qsSet;

		List<QSEntity> listQsSet = queryStructure.getList(Type.SET);

		for (QSEntity entity : listQsSet) {
			qsSet = (QSSet) entity;

			gam.recordUpdate(idRootNode, qsSet.getProperty(), qsSet.getNewValue());
		}

		sendById(queryStructure, idRootNode);
	}

	/**
	 * Process and group the results of the query's execution through the system.
	 * @param resultQuery the results of the query's execution.
	 * @param queryStructure
	 * @param trackingMode indicates if the queries is subgenerated from the original query and it's is part of a recursive
	 *                     process.
	 */
	public void processQueryResults(ResultQuery resultQuery, QueryStructure queryStructure, boolean trackingMode) {
		int queryType = queryStructure.getQueryType();

		if (resultQuery == null) return;

		if (this.initialResultQuery == null) {
			this.initialResultQuery = new ResultQuery(resultQuery.getColumnsName());
		}

		switch (queryType) {
			case QueryStructure.QUERY_TYPE_CHAINED:
				chainedQueryResult(resultQuery, queryStructure, trackingMode);
				break;
			default:
				defaultQueryResult(resultQuery, queryStructure, trackingMode);
		}
	}

	/**
	 * Send a query to the original root node partition to get its information. This information is needed to replace
	 * some conditions in the WHERE clause related with the root node.
	 * EX:
	 * ORG Query				MOD Query
	 * (root.age > m.age)	=>	(14 > m.age)
	 *
	 * @param queryStructure
	 */
	private void getRootNodeDetails(QueryStructure queryStructure) {

		int idPartitionLocal = MetadataManager.getInstance().getMapGraphNodes().get(queryStructure.getRootNodeId());
		String queryRootInfo = "MATCH (n {id:" + queryStructure.getRootNodeId() + " }) RETURN n;";
		ResultQuery resultQueryRootInfo;

		if (idPartitionLocal == 0) {
			resultQueryRootInfo = queryExecutor.processQuery(queryRootInfo);
		} else {
			resultQueryRootInfo = mmServer.sendStringQuery(idPartitionLocal, queryRootInfo);
		}

		if (resultQueryRootInfo.getColumnsCount() > 0) {
			List<ResultEntity> column = resultQueryRootInfo.getColumn(0);

			if (column.size() > 0) {
				rootNode = (ResultNode) column.get(0);
			}
		}
	}

	/**
	 * Manage the query result of a default query.
	 * @param resultQuery
	 * @param queryStructure
	 * @param trackingMode
	 */
	private void defaultQueryResult(ResultQuery resultQuery, QueryStructure queryStructure, boolean trackingMode) {
		Iterator it;
		int indexOrgColumn;
		int columnsCount = resultQuery.getColumnsCount();
		ResultNode resultNode = null;

		if (resultQuery.getColumnsCount() <= 0) return;
		if (queryStructure.getQueryType() == QueryStructure.QUERY_TYPE_BROADCAST) broadcastsReceived++;

		if (!trackingMode &&
				(queryStructure.getQueryType() != QueryStructure.QUERY_TYPE_CREATE) &&
				(queryStructure.getQueryType() != QueryStructure.QUERY_TYPE_BROADCAST) &&
				(queryStructure.getQueryType() != QueryStructure.QUERY_TYPE_UPDATE)) {

			getRootNodeDetails(queryStructure);

		}

		if (!trackingMode && (queryStructure.getMatchVariablesCount() == originalQueryStructure.getMatchVariablesCount())) {
			initialResultQuery.setColumnsName(resultQuery.getColumnsName());
		}

		List<ResultEntity> firstColResults = resultQuery.getColumn(0);
		int firstColResultsSize = 0;
		if (firstColResults != null) firstColResultsSize = firstColResults.size();

		for (int i = 0; i < firstColResultsSize; i++) {

			for (int j = 0; j < columnsCount; j++) {
				ResultEntity colResult = resultQuery.getColumn(j).get(i);

				indexOrgColumn = initialResultQuery.getColumnsName().indexOf(resultQuery.getColumnsName().get(j));


				if (colResult instanceof ResultNode) {
					resultNode = (ResultNode) colResult;

					if (resultNode.isBorderNode()) {
						if (queryStructure.getQueryType() != QueryStructure.QUERY_TYPE_BROADCAST) {

							int matchVarLevel = queryStructure.getNodeLevel(initialResultQuery.getColumnsName().get(i));

							/*
							En el border node actual tengo información del id de la particion a la cual esta sirviendo como embajador.
							Usando el objeto queryStructure podemos recuperar en id del Root node actual y con este id obtener la partición actual
							Con ambas particiones tenemos la key para recuperar el id del border node en la partición forastera
							 */
							int idPartitionLocal = MetadataManager.getInstance().getMapGraphNodes().get(queryStructure.getRootNodeId());
							int idPartitionForeign = resultNode.getForeignPartitionId();

							int idForeignBorderNode = MetadataManager.getInstance().getMapBoarderNodes().getBorderNodeID(idPartitionForeign, idPartitionLocal);

							QueryStructure queryStructureModified = queryStructure.replaceRootNode(idForeignBorderNode, rootNode, matchVarLevel);

							if (idPartitionForeign == 0) {
								queryExecutor.processQuery(queryStructureModified, this, true);
							} else {
								mmServer.sendQuery(idPartitionForeign, queryStructureModified, this, true);
							}
						}
					} else {
						if (trackingMode) {
							// Add only the node if has relation with the root node
							if (relationshipsTable.existsRelationship(queryStructure.getRootNodeId(), resultNode.getNodeId(), rootNode.getNodeId())) {
								initialResultQuery.addEntity(indexOrgColumn, resultNode);
							}
						} else {
							initialResultQuery.addEntity(j, resultNode);
						}
					}

				} else if (colResult instanceof ResultRelation) {
					if (!trackingMode && !resultNode.isBorderNode()) {
						ResultRelation resultRelation = (ResultRelation) colResult;
						it = resultRelation.getProperties().entrySet().iterator();
						while (it.hasNext()) {
							Map.Entry entry = (Map.Entry) it.next();
						}

						// If one of the nodes is border, delete the property related with the foreign node.
						if (resultRelation.getProperties().containsKey("idRelForeignNode"))
							resultRelation.getProperties().remove("idRelForeignNode");
						initialResultQuery.addEntity(indexOrgColumn, colResult);
					}
				} else {
					// Value
					initialResultQuery.addEntity(indexOrgColumn, colResult);
					System.out.println("VALUE!");
				}
			}


			if (!trackingMode) {
				int difference;

				for (int k = (initialResultQuery.getColumnsCount() - 1); k >= 1; k--) {
					difference = initialResultQuery.getColumn(k).size() - initialResultQuery.getColumn(k - 1).size();

					// No results in the last column
					if (difference < 0) {
						initialResultQuery.removeLast(k - 1);
					}

					for (int l = 0; l < difference; l++) {
						if (initialResultQuery.getColumn(k - 1).size() > 0) {
							initialResultQuery.addEntity(k - 1, initialResultQuery.getColumn(k - 1).get(initialResultQuery.getColumn(k - 1).size() - 1));
						}
					}
				}
			}
		}

		if ((!trackingMode && queryStructure.getQueryType() != QueryStructure.QUERY_TYPE_BROADCAST ||
				(queryStructure.getQueryType() == QueryStructure.QUERY_TYPE_BROADCAST && broadcastsReceived == MetadataManager.getInstance().getMMInformation().getNumberPartitions()))) {
			// Show result table
			TextTable textTable = new TextTable((String[]) initialResultQuery.getColumnsName().toArray(), initialResultQuery.getDataTable());
			textTable.printTable();
			System.out.println("\n\n");
		}
	}

	/**
	 * Manage the query result of a chained query.
	 * @param resultQuery
	 * @param queryStructure
	 * @param trackingMode
	 */
	private void chainedQueryResult(ResultQuery resultQuery, QueryStructure queryStructure, boolean trackingMode) {
		int columnsCount = resultQuery.getColumnsCount();
		Map<Integer, ResultEntity> tempResultQuery = new HashMap<>();
		int localLastChainedId;

		// The subqueries may not have the same number of column as the original query. Is important add the new result in the
		// corresponding column to show the results

		if (!trackingMode && (queryStructure.getMatchVariablesCount() == originalQueryStructure.getMatchVariablesCount())) {
			initialResultQuery.setColumnsName(resultQuery.getColumnsName());
		}

		List<ResultEntity> firstColResults = resultQuery.getColumn(0);
		int firstColResultsSize = firstColResults.size();

		for (int i = 0; i < firstColResultsSize; i++) {
			tempResultQuery.clear();

			for (int j = 0; j < columnsCount; j++) {
				ResultEntity colResult = resultQuery.getColumn(j).get(i);

				if (colResult instanceof ResultNode) {
					ResultNode node = (ResultNode) colResult;

					if (node.isBorderNode()) {
						if (j == (columnsCount - 1)) {

							int matchVarLevel = queryStructure.getMatchVariablesCount();
							if (!exploredBorderNodes.contains(node.getNodeId() + "-" + matchVarLevel)) {
								exploredBorderNodes.add(node.getNodeId() + "-" + matchVarLevel);

								int idPartitionLocal = MetadataManager.getInstance().getMapGraphNodes().get(queryStructure.getRootNodeId());
								int idPartitionForeign = node.getForeignPartitionId();

								int idForeignBorderNode = MetadataManager.getInstance().getMapBoarderNodes().getBorderNodeID(idPartitionForeign, idPartitionLocal);

								int borderVarIndex = initialResultQuery.getColumnsName().indexOf(resultQuery.getColumnsName().get(j));


								localLastChainedId = chainedLastNodeId;
								if (tempResultQuery.get(j - 1) instanceof ResultNode) {
									chainedLastNodeId = (int) ((ResultNode) tempResultQuery.get(j - 1)).getProperties().get("id");
								}

								int end = originalQueryStructure.getMatchVariablesCount();

								do {
									QueryStructure queryStructureModified = originalQueryStructure.getSubChainQuery(borderVarIndex, end - 1, idForeignBorderNode, chainedLastNodeId);

									if (idPartitionForeign == 0) {
										queryExecutor.processQuery(queryStructureModified, this, true);
									} else {
										mmServer.sendQuery(idPartitionForeign, queryStructureModified, this, true);
									}
									end--;
								} while ((end - borderVarIndex) >= 2);

								chainedLastNodeId = localLastChainedId;

								if (explorationWithResults == 0) {
									tempResultQuery.clear();
								} else {

									Set<Map.Entry<Integer, ResultEntity>> set = tempResultQuery.entrySet();

									for (Map.Entry<Integer, ResultEntity> result : set) {
										initialResultQuery.addEntity(initialResultQuery.getColumnsName().indexOf(resultQuery.getColumnsName().get(result.getKey())), result.getValue());
									}

									tempResultQuery.clear();
								}
							}
						} else {
							tempResultQuery.clear();
							break;
						}

					} else {
						int currentNodeID = node.getNodeId();
						RelationshipsTable relationshipsTable = MetadataManager.getInstance().getRelationshipsTable();
						Map<Integer, Integer> mapGraphNodes = MetadataManager.getInstance().getMapGraphNodes();
						MapBorderNodes mapBorderNodes = MetadataManager.getInstance().getMapBoarderNodes();

						int partitionCurrentNode = mapGraphNodes.get(currentNodeID);

						int partitionChainedLastNode = mapGraphNodes.get(chainedLastNodeId);

						if (j == (resultQuery.getColumnsCount() - 1) && !(resultQuery.getColumnsName().get(j).equals(initialResultQuery.getColumnsName().get(initialResultQuery.getColumnsCount() - 1)))) {
							tempResultQuery.clear();
							break;
						}

						if ((trackingMode && j == 0 && relationshipsTable.existsRelationship(mapBorderNodes.getBorderNodeID(partitionChainedLastNode, partitionCurrentNode), currentNodeID, chainedLastNodeId)
								|| (trackingMode && j > 0)
								|| !trackingMode)) {
							tempResultQuery.put(j, node);

							if (resultQuery.getColumnsName().get(j).equals(initialResultQuery.getColumnsName().get(initialResultQuery.getColumnsCount() - 1))) {

								Set<Map.Entry<Integer, ResultEntity>> set = tempResultQuery.entrySet();

								for (Map.Entry<Integer, ResultEntity> result : set) {
									initialResultQuery.addEntity(initialResultQuery.getColumnsName().indexOf(resultQuery.getColumnsName().get(result.getKey())), result.getValue());
								}

								if (trackingMode) explorationWithResults++;

								tempResultQuery.clear();

							}
						} else {

							tempResultQuery.clear();
							break;
						}
					}
				}
			}

			if (!trackingMode) {

				int difference;

				for (int k = (initialResultQuery.getColumnsCount() - 1); k >= 1; k--) {
					difference = initialResultQuery.getColumn(k).size() - initialResultQuery.getColumn(k - 1).size();

					for (int l = 0; l < difference; l++) {
						if (initialResultQuery.getColumn(k - 1).size() > 0) {
							initialResultQuery.addEntity(k - 1, initialResultQuery.getColumn(k - 1).get(initialResultQuery.getColumn(k - 1).size() - 1));

						}
					}
				}
			}
		}

		// Is the last derived sub-query sent
		if (!trackingMode && (queryStructure.getMatchVariablesCount() == 2)) {
			// Show result table
			TextTable textTable = new TextTable((String[]) initialResultQuery.getColumnsName().toArray(), initialResultQuery.getDataTable());
			textTable.printTable();
			System.out.println("\n\n");
		}
	}
}
