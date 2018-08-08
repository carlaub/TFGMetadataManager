package controllers;

import application.MetadataManager;
import constants.ErrorConstants;
import constants.GenericConstants;
import data.MapBorderNodes;
import dnl.utils.text.table.TextTable;
import managers.GraphAlterationsManager;
import neo4j.*;
import network.MMServer;
import parser.Type;
import queryStructure.*;
import relationsTable.RelationshipsTable;

import java.util.*;

/**
 * Created by Carla Urrea Blázquez on 06/07/2018.
 * <p>
 * QueriesControlles.java
 * <p>
 * For each query, create a controller.
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

	public void sendById(QueryStructure queryStructure, int idRootNode) {
		sendById(queryStructure, idRootNode, false);
	}

	public void sendById(QueryStructure queryStructure, int idRootNode, boolean trackingMode) {
//		System.out.println("ID Root node: " + idRootNode);
		int partitionID = MetadataManager.getInstance().getMapGraphNodes().get(idRootNode);
		System.out.println("\nSend to partition: " + partitionID);
		sendByPartitionID(queryStructure, partitionID, trackingMode);
	}


	public void sendByPartitionID(QueryStructure queryStructure, int partitionID) {
		sendByPartitionID(queryStructure, partitionID, false);
	}

	public void sendByPartitionID(QueryStructure queryStructure, int partitionID, boolean trackingMode) {
		if (partitionID == GenericConstants.MM_SLAVE_NODE_ID) {
			// Root node is inside MetadataManager's Neo4j instance
			queryExecutor.processQuery(queryStructure, this, trackingMode);
		} else {
			// Root node is in the slave node with id "partitionID"
			mmServer.sendQuery(partitionID, queryStructure, this, trackingMode);
		}
	}

	public void manageNewQuery(QueryStructure queryStructure) {
		int queryType = queryStructure.getQueryType();
		int idRootNode;

		originalQueryStructure = queryStructure;
		System.out.println("Query Type: " + queryStructure.getQueryType());

		switch (queryType) {
			case QueryStructure.QUERY_TYPE_CREATE:
				manageQueryCreate(queryStructure);
				break;

			case QueryStructure.QUERY_TYPE_DETACH:
				manageQueryDelete(queryStructure);
				break;

			case QueryStructure.QUERY_TYPE_CHAINED:
				System.out.println("--> QUERY CHAINED");

				idRootNode = queryStructure.getRootNodeId();
//				System.out.println("ID root node: " + idRootNode);
				int matchVarsCount = queryStructure.getMatchVariablesCount();
//				System.out.println("Variables Count: " + matchVarsCount);

				// Derived Sub-queries
				for (int i = (matchVarsCount - 1); i > 0; i--) {
					chainedLastNodeId = originalQueryStructure.getRootNodeId();
					System.out.println("\n--> Subquery chained: \n" + queryStructure.getSubChainQuery(0, i, -1, -1).toString());
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
				System.out.println("ID root node: " + idRootNode);


				if (idRootNode > 0) {
					System.out.println("\n--> ENVIO: \n" + queryStructure.toString());
					sendById(queryStructure, idRootNode);
				} else {
					System.out.println("--> BROADCAST");
					// CASE 2: Query's MATCH clause has not a relation
					broadcastsReceived = 0;

					queryStructure.setQueryType(QueryStructure.QUERY_TYPE_BROADCAST);
					mmServer.sendQueryBroadcast(queryStructure, this);
					queryExecutor.processQuery(queryStructure, this, false);
				}
				break;

		}
	}

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

	public void manageQueryDelete(QueryStructure queryStructure) {
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

	public void manageQuerySet(QueryStructure queryStructure) {
		int idRootNode = queryStructure.getRootNodeId();
		QSSet qsSet;

		List<QSEntity> listQsSet = queryStructure.getList(Type.SET);

		for (QSEntity entity : listQsSet) {
			qsSet = (QSSet) entity;

			gam.recordUpdate(idRootNode, qsSet.getProperty(), qsSet.getNewValue());
		}

		sendById(queryStructure, idRootNode);
	}

	public void processQueryResults(ResultQuery resultQuery, QueryStructure queryStructure, boolean trackingMode) {
		int queryType = queryStructure.getQueryType();

		System.out.println("-> Query Result received");

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


//	private void defaultQueryResult(ResultQuery resultQuery, QueryStructure queryStructure, boolean trackingMode) {
//		Iterator it;
//		int indexOrgColumn = 0;
//
//		if (!trackingMode &&
//				(queryStructure.getQueryType() != QueryStructure.QUERY_TYPE_CREATE) &&
//				(queryStructure.getQueryType() != QueryStructure.QUERY_TYPE_BROADCAST)) {
//
//			//TODO: create retrieveRootNodeInformation function
//			// Send a query to the original root node partition to get its information. This information is needed to replace
//			// some conditions in the WHERE clause related with the root node
//			// EX:
//			//	ORG Query				MOD Query
//			// (root.age > m.age)	=>	(14 > m.age)
//
//			System.out.println("Search for root node id: " + queryStructure.getRootNodeId());
//
//			int idPartitionLocal = MetadataManager.getInstance().getMapGraphNodes().get(queryStructure.getRootNodeId());
//			String queryRootInfo = "MATCH (n {id:" + queryStructure.getRootNodeId() + " }) RETURN n;";
//			ResultQuery resultQueryRootInfo;
//
//			if (idPartitionLocal == 0) {
//				resultQueryRootInfo = queryExecutor.processQuery(queryRootInfo);
//			} else {
//				resultQueryRootInfo = mmServer.sendStringQuery(idPartitionLocal, queryRootInfo);
//			}
//
//			if (resultQueryRootInfo.getColumnsCount() > 0) {
//				List<ResultEntity> column = resultQueryRootInfo.getColumn(0);
//
//				if (column.size() > 0) {
//					rootNode = (ResultNode) column.get(0);
//				}
//			}
//
//		}
//
//		int columnsCount = resultQuery.getColumnsCount();
//		// The subqueries may not have the same number of column as the original query. Is important add the new result in the
//		// corresponding column to show the results
//
////		System.out.println("\nResults default: ");
////		TextTable textTable2 = new TextTable((String[]) resultQuery.getColumnsName().toArray(), resultQuery.getDataTable());
////		textTable2.printTable();
////		System.out.println("\n\n");
//
//		for (int i = 0; i < columnsCount; i++) {
//			List<ResultEntity> columnResults = resultQuery.getColumn(i);
//			System.out.println("Column " + i + " size: " + columnResults.size() + " " + trackingMode);
//
//			if (trackingMode)
//				indexOrgColumn = initialResultQuery.getColumnsName().indexOf(resultQuery.getColumnsName().get(i));
//
//			for (ResultEntity result : columnResults) {
//
//				if (result instanceof ResultNode) {
//
//					System.out.println("Entra en node");
//
//					// TODO: Si es un nodo frontera, hacer el sendById query pasando la misma instanci y borrar el nodo frontera de la query,
//					// activar el modo tracking de sendQuery para concatenar los nuevos resultados y no mostrar aun la tabla al usuario.
//
//					ResultNode resultNode = (ResultNode) result;
//
//
//					if (resultNode.isBorderNode()) {
//						if (queryStructure.getQueryType() != QueryStructure.QUERY_TYPE_BROADCAST) {
//
//							int matchVarLevel = queryStructure.getNodeLevel(initialResultQuery.getColumnsName().get(i));
//
//							if (!exploredBorderNodes.contains(resultNode.getNodeId() + "-" + matchVarLevel)) {
//
//								exploredBorderNodes.add(resultNode.getNodeId() + "-" + matchVarLevel);
//								/*
//								En el border node actual tengo información del id de la particion a la cual esta sirviendo como embajador.
//								Usando el objeto queryStructure podemos recuperar en id del Root node actual y con este id obtener la partición actual
//								Con ambas particiones tenemos la key para recuperar el id del border node en la partición forastera
//								 */
//								int idPartitionLocal = MetadataManager.getInstance().getMapGraphNodes().get(queryStructure.getRootNodeId());
//								int idPartitionForeign = resultNode.getForeignPartitionId();
//
//								int idForeignBorderNode = MetadataManager.getInstance().getMapBoarderNodes().getBorderNodeID(idPartitionForeign, idPartitionLocal);
//
//								QueryStructure queryStructureModified = queryStructure.replaceRootNode(idForeignBorderNode, rootNode, matchVarLevel);
//
//								if (idPartitionForeign == 0) {
//									queryExecutor.processQuery(queryStructureModified, this, true);
//								} else {
//									mmServer.sendQuery(idPartitionForeign, queryStructureModified, this, true);
//								}
//
//								System.out.println("Salgo de border. Tracking: " + trackingMode);
//
//							}
//						}
//					} else {
//						if (trackingMode) {
//							// Add only the node if has relation with the root node
//							if (relationshipsTable.existsRelationship(queryStructure.getRootNodeId(), resultNode.getNodeId(), rootNode.getNodeId())) {
//								initialResultQuery.addEntity(indexOrgColumn, result);
//							}
//						} else {
//							System.out.println("Add entity: " + i);
//							initialResultQuery.addEntity(i, result);
//						}
//					}
//
//				} else if (result instanceof ResultRelation) {
//					System.out.println("Hay relacion tracking mode: " + trackingMode);
//					if (!trackingMode) {
//						System.out.println("Entro en la relacion");
//						ResultRelation resultRelation = (ResultRelation) result;
//						it = resultRelation.getProperties().entrySet().iterator();
//						while (it.hasNext()) {
//							Map.Entry entry = (Map.Entry) it.next();
//							System.out.println("- " + entry.getKey() + ": " + entry.getValue());
//						}
//						System.out.println("\n");
//						initialResultQuery.addEntity(i, result);
//					}
//				} else {
//					// Value
//					initialResultQuery.addEntity(i, result);
//				}
//			}
//		}
//
//		if (queryStructure.getQueryType() == QueryStructure.QUERY_TYPE_BROADCAST) broadcastsReceived++;
//
//		if ((!trackingMode && queryStructure.getQueryType() != QueryStructure.QUERY_TYPE_BROADCAST ||
//				(queryStructure.getQueryType() == QueryStructure.QUERY_TYPE_BROADCAST && broadcastsReceived == MetadataManager.getInstance().getMMInformation().getNumberPartitions()))) {
//			// Show result table
//			TextTable textTable = new TextTable((String[]) initialResultQuery.getColumnsName().toArray(), initialResultQuery.getDataTable());
//			textTable.printTable();
//			System.out.println("\n\n");
//		}
//	}

	private void defaultQueryResult(ResultQuery resultQuery, QueryStructure queryStructure, boolean trackingMode) {
		Iterator it;
		int indexOrgColumn = 0;
		Map<Integer, ResultEntity> tempResultQuery = new HashMap<>();
		int columnsCount = resultQuery.getColumnsCount();

		if (!trackingMode &&
				(queryStructure.getQueryType() != QueryStructure.QUERY_TYPE_CREATE) &&
				(queryStructure.getQueryType() != QueryStructure.QUERY_TYPE_BROADCAST) &&
				(queryStructure.getQueryType() != QueryStructure.QUERY_TYPE_UPDATE)) {

			//TODO: create retrieveRootNodeInformation function
			// Send a query to the original root node partition to get its information. This information is needed to replace
			// some conditions in the WHERE clause related with the root node
			// EX:
			//	ORG Query				MOD Query
			// (root.age > m.age)	=>	(14 > m.age)

			System.out.println("Search for root node id: " + queryStructure.getRootNodeId());

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
					System.out.println("Entra en node");

					// TODO: Si es un nodo frontera, hacer el sendById query pasando la misma instanci y borrar el nodo frontera de la query,
					// activar el modo tracking de sendQuery para concatenar los nuevos resultados y no mostrar aun la tabla al usuario.

					ResultNode resultNode = (ResultNode) colResult;


					if (resultNode.isBorderNode()) {
						if (queryStructure.getQueryType() != QueryStructure.QUERY_TYPE_BROADCAST) {

							int matchVarLevel = queryStructure.getNodeLevel(initialResultQuery.getColumnsName().get(i));

							if (!exploredBorderNodes.contains(resultNode.getNodeId() + "-" + matchVarLevel)) {

								exploredBorderNodes.add(resultNode.getNodeId() + "-" + matchVarLevel);
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

								System.out.println("Salgo de border. Tracking: " + trackingMode);

							}
						}
					} else {
						if (trackingMode) {
							// Add only the node if has relation with the root node
							if (relationshipsTable.existsRelationship(queryStructure.getRootNodeId(), resultNode.getNodeId(), rootNode.getNodeId())) {
								initialResultQuery.addEntity(indexOrgColumn, resultNode);
							}
						} else {
							System.out.println("Add entity: " + i);
							initialResultQuery.addEntity(i, resultNode);
						}
					}

				} else if (colResult instanceof  ResultRelation) {
					System.out.println("Hay relacion tracking mode: " + trackingMode);
					if (!trackingMode) {
						System.out.println("Entro en la relacion");
						ResultRelation resultRelation = (ResultRelation) colResult;
						it = resultRelation.getProperties().entrySet().iterator();
						while (it.hasNext()) {
							Map.Entry entry = (Map.Entry) it.next();
							System.out.println("- " + entry.getKey() + ": " + entry.getValue());
						}
						System.out.println("\n");
						initialResultQuery.addEntity(i, colResult);
					}
				} else {
					// Value
					initialResultQuery.addEntity(i, colResult);
				}


				if (queryStructure.getQueryType() == QueryStructure.QUERY_TYPE_BROADCAST) broadcastsReceived++;

				if ((!trackingMode && queryStructure.getQueryType() != QueryStructure.QUERY_TYPE_BROADCAST ||
						(queryStructure.getQueryType() == QueryStructure.QUERY_TYPE_BROADCAST && broadcastsReceived == MetadataManager.getInstance().getMMInformation().getNumberPartitions()))) {
					// Show result table
					TextTable textTable = new TextTable((String[]) initialResultQuery.getColumnsName().toArray(), initialResultQuery.getDataTable());
					textTable.printTable();
					System.out.println("\n\n");
				}
			}

			if (!trackingMode) {
				int difference;

				for (int k = (initialResultQuery.getColumnsCount() - 1); k >= 1; k--) {
					difference = initialResultQuery.getColumn(k).size() - initialResultQuery.getColumn(k-1).size();

					for (int l = 0; l < difference; l++) {
						if (initialResultQuery.getColumn(k-1).size() > 0) {
							initialResultQuery.addEntity(k-1, initialResultQuery.getColumn(k-1).get(initialResultQuery.getColumn(k-1).size()-1));

							System.out.println("Add entity col rep : " + (k-1) + " - " + initialResultQuery.getColumn(k-1).get(initialResultQuery.getColumn(k-1).size()-1));

						}
					}
				}
			}
		}

		// Is the last derived sub-query sent
		if (!trackingMode) {
			// Show result table
			TextTable textTable = new TextTable((String[]) initialResultQuery.getColumnsName().toArray(), initialResultQuery.getDataTable());
			textTable.printTable();
			System.out.println("\n\n");
		}
	}

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
//
//		System.out.println("\nResults: ");
//		TextTable textTable2 = new TextTable((String[]) resultQuery.getColumnsName().toArray(), resultQuery.getDataTable());
//		textTable2.printTable();
//		System.out.println("\n\n");

		int fetchedResults = 0;

		for (int i = 0; i < firstColResultsSize; i++) {
			tempResultQuery.clear();
			for (int j = 0; j < columnsCount; j++) {
				ResultEntity colResult = resultQuery.getColumn(j).get(i);

				if (colResult instanceof ResultNode) {
					ResultNode node = (ResultNode) colResult;

					if (node.isBorderNode()) {
						if (j == (columnsCount - 1)) {
							System.out.println("\n--> Border Node id: " + node.getNodeId() + " partition fore: " + node.getProperties().get("partition"));

							int matchVarLevel = queryStructure.getMatchVariablesCount();
							if (!exploredBorderNodes.contains(node.getNodeId() + "-" + matchVarLevel)) {
								exploredBorderNodes.add(node.getNodeId() + "-" + matchVarLevel);
								System.out.println("---> Explored: " + node.getNodeId() + "-" + matchVarLevel);

								int idPartitionLocal = MetadataManager.getInstance().getMapGraphNodes().get(queryStructure.getRootNodeId());
								int idPartitionForeign = node.getForeignPartitionId();

								int idForeignBorderNode = MetadataManager.getInstance().getMapBoarderNodes().getBorderNodeID(idPartitionForeign, idPartitionLocal);

								int borderVarIndex = initialResultQuery.getColumnsName().indexOf(resultQuery.getColumnsName().get(j));


								localLastChainedId = chainedLastNodeId;
								if (tempResultQuery.get(j-1) instanceof ResultNode) {
									chainedLastNodeId = (int)((ResultNode)tempResultQuery.get(j-1)).getProperties().get("id");
								}

								System.out.println("--> Border Var Index: " + borderVarIndex + "  -  id Foreign: " + idForeignBorderNode);

								int end = originalQueryStructure.getMatchVariablesCount();

								do {
									QueryStructure queryStructureModified = originalQueryStructure.getSubChainQuery(borderVarIndex, end - 1, idForeignBorderNode, chainedLastNodeId);
									System.out.println("--> QueryModified: " + queryStructureModified);
	//								explorationWithResults = 0;

									if (idPartitionForeign == 0) {
										queryExecutor.processQuery(queryStructureModified, this, true);
									} else {
										mmServer.sendQuery(idPartitionForeign, queryStructureModified, this, true);
									}
									end --;
								} while ((end - borderVarIndex) >= 2);

								System.out.println("Salgo de border. Tracking: " + trackingMode + " exploration count: " + explorationWithResults);

								chainedLastNodeId = localLastChainedId;

								System.out.println("De la query: " + queryStructure.toString());
								if (explorationWithResults == 0) {
									tempResultQuery.clear();
								} else {

									Set<Map.Entry<Integer, ResultEntity>> set = tempResultQuery.entrySet();

									for (Map.Entry<Integer, ResultEntity> result : set) {
//										for (int k = 0; k < difference; k++) {
											System.out.println("Add entity column: " + initialResultQuery.getColumnsName().indexOf(resultQuery.getColumnsName().get(result.getKey())) + " - " + result.getValue());
											initialResultQuery.addEntity(initialResultQuery.getColumnsName().indexOf(resultQuery.getColumnsName().get(result.getKey())), result.getValue());
//										}
									}

									tempResultQuery.clear();
								}
							}
						} else {
							tempResultQuery.clear();
							break;
						}

					} else {
						// TODO: hacer una funcion getColumnByName
						// TODO: check si el nodo tiene relacion con el ultimo nodo procesado
						int currentNodeID = node.getNodeId();
						RelationshipsTable relationshipsTable = MetadataManager.getInstance().getRelationshipsTable();
						Map<Integer, Integer> mapGraphNodes = MetadataManager.getInstance().getMapGraphNodes();
						MapBorderNodes mapBorderNodes = MetadataManager.getInstance().getMapBoarderNodes();

						int partitionCurrentNode = mapGraphNodes.get(currentNodeID);
						System.out.println("\n--> ChainedLastNode ID: " + chainedLastNodeId);

						int partitionChainedLastNode = mapGraphNodes.get(chainedLastNodeId);

						if (j == (resultQuery.getColumnsCount() - 1) && !(resultQuery.getColumnsName().get(j).equals(initialResultQuery.getColumnsName().get(initialResultQuery.getColumnsCount() - 1)))) {
							tempResultQuery.clear();
							break;
						}

						if ((trackingMode && j == 0 && relationshipsTable.existsRelationship(mapBorderNodes.getBorderNodeID(partitionChainedLastNode, partitionCurrentNode), currentNodeID, chainedLastNodeId)
						 || (trackingMode && j > 0)
								|| !trackingMode)) {
							System.out.println("add");
							tempResultQuery.put(j , node);

							System.out.println("\n--> PastChainedLastNode ID: " + chainedLastNodeId);



//							if (j == (columnsCount - 1)) {
							System.out.println("\n --> PROB: " + resultQuery.getColumnsName().get(j) + " equals " + initialResultQuery.getColumnsName().get(initialResultQuery.getColumnsCount() - 1));

							if (resultQuery.getColumnsName().get(j).equals(initialResultQuery.getColumnsName().get(initialResultQuery.getColumnsCount() - 1))){
								fetchedResults ++;

								Set<Map.Entry<Integer, ResultEntity>> set = tempResultQuery.entrySet();

								for (Map.Entry<Integer, ResultEntity> result : set) {
									System.out.println("Add part 2, col: " + initialResultQuery.getColumnsName().indexOf(resultQuery.getColumnsName().get(result.getKey())) + " - " + result.getValue());
									initialResultQuery.addEntity(initialResultQuery.getColumnsName().indexOf(resultQuery.getColumnsName().get(result.getKey())), result.getValue());
								}

								if (trackingMode) explorationWithResults++;

								tempResultQuery.clear();

								// Reset to the initial id
//								chainedLastNodeId = originalQueryStructure.getRootNodeId();
							} else {
//								chainedLastNodeId = node.getNodeId();
								System.out.println("-> ChainedLastNode ID ELSE: " + chainedLastNodeId);
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
					difference = initialResultQuery.getColumn(k).size() - initialResultQuery.getColumn(k-1).size();

					for (int l = 0; l < difference; l++) {
						if (initialResultQuery.getColumn(k-1).size() > 0) {
							initialResultQuery.addEntity(k-1, initialResultQuery.getColumn(k-1).get(initialResultQuery.getColumn(k-1).size()-1));

							System.out.println("Add entity col rep : " + (k-1) + " - " + initialResultQuery.getColumn(k-1).get(initialResultQuery.getColumn(k-1).size()-1));

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
