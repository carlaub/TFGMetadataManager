package controllers;

import application.MetadataManager;
import constants.ErrorConstants;
import constants.GenericConstants;
import dnl.utils.text.table.TextTable;
import managers.GraphAlterationsManager;
import neo4j.*;
import network.MMServer;
import queryStructure.QSNode;
import queryStructure.QSRelation;
import queryStructure.QueryStructure;
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


	public QueriesController() {
		mmServer = MMServer.getInstance();
		queryExecutor = new QueryExecutor();
		initialResultQuery = null;
		rootNode = null;
		relationshipsTable = MetadataManager.getInstance().getRelationshipsTable();
		exploredBorderNodes = new ArrayList<>();
	}

	public void sendById(QueryStructure queryStructure, int idRootNode) {
		System.out.println("ID Root node: " + idRootNode);
		int partitionID = MetadataManager.getInstance().getMapGraphNodes().get(idRootNode);

		sendByPartitionID(queryStructure, partitionID);
	}

	public void sendByPartitionID(QueryStructure queryStructure, int partitionID) {
		if (partitionID == GenericConstants.MM_SLAVE_NODE_ID) {
			// Root node is inside MetadataManager's Neo4j instance
			queryExecutor.processQuery(queryStructure, this, false);
		} else {
			// Root node is in the slave node with id "partitionID"
			mmServer.sendQuery(partitionID, queryStructure, this, false);
		}
	}

	public void manageNewQuery(QueryStructure queryStructure) {
		int queryType = queryStructure.getQueryType();
		int partition;
		GraphAlterationsManager gam = GraphAlterationsManager.getInstance();

		if (queryType == QueryStructure.QUERY_TYPE_CREATE) {
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
						//TODO: TEST Enviar las queries a todas las particiones que sea requerido
						partition = entry.getKey();
						if (partition == 0) {
							// Local Neo4j DB
							queryExecutor.processQuery(entry.getValue());
						} else {
							mmServer.sendStringQuery(entry.getKey(), entry.getValue());
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

		} else if (queryType == QueryStructure.QUERY_TYPE_DEFAULT) {
			// CASE 1: Query's MATCH clause has a relation
			int idRootNode = queryStructure.getRootNodeId();
			System.out.println("ID root node: " + idRootNode);


			if (idRootNode > 0) {
				sendById(queryStructure, idRootNode);
			} else {
				System.out.println("--> BROADCAST");
				// CASE 2: Query's MATCH clause has not a relation
				queryStructure.setQueryType(QueryStructure.QUERY_TYPE_BROADCAST);
				mmServer.sendQueryBroadcast(queryStructure, this);
				queryExecutor.processQuery(queryStructure, this, false);
			}
		}
	}

	public void processQueryResults(ResultQuery resultQuery, QueryStructure queryStructure, boolean trackingMode) {
		int queryType = queryStructure.getQueryType();

		System.out.println("-> Query Result received");

		if (resultQuery == null) return;

		if (this.initialResultQuery == null) {
			this.initialResultQuery = new ResultQuery(resultQuery.getColumnsName());
		}

		defaultQueryResult(resultQuery, queryStructure, trackingMode);

//		switch (queryType) {
//			case QueryStructure.QUERY_TYPE_DEFAULT:
//				defaultQueryResult(resultQuery, queryStructure, trackingMode);
//				break;
//			case QueryStructure.QUERY_TYPE_CREATE:
//				createQueryResult(resultQuery);
//				break;
//			case QueryStructure.QUERY_TYPE_DELETE:
//				deleteQueryResult(resultQuery, queryStructure);
//				break;
//		}
	}


	private void defaultQueryResult(ResultQuery resultQuery, QueryStructure queryStructure, boolean trackingMode) {
		Iterator it;
		int indexOrgColumn = 0;

		if (!trackingMode &&
				(queryStructure.getQueryType() != QueryStructure.QUERY_TYPE_CREATE) &&
				(queryStructure.getQueryType() != QueryStructure.QUERY_TYPE_BROADCAST)) {
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

		int columnsCount = resultQuery.getColumnsCount();
		// The subqueries may not have the same number of column as the original query. Is important add the new result in the
		// corresponding column to show the results

		for (int i = 0; i < columnsCount; i++) {
			List<ResultEntity> columnResults = resultQuery.getColumn(i);
			System.out.println("Column " + i + " size: " + columnResults.size() + " " + trackingMode);

			if (trackingMode)
				indexOrgColumn = initialResultQuery.getColumnsName().indexOf(resultQuery.getColumnsName().get(i));

			for (ResultEntity result : columnResults) {

				if (result instanceof ResultNode) {

					System.out.println("Entra en node");

					// TODO: Si es un nodo frontera, hacer el sendById query pasando la misma instanci y borrar el nodo frontera de la query,
					// activar el modo tracking de sendQuery para concatenar los nuevos resultados y no mostrar aun la tabla al usuario.

					ResultNode resultNode = (ResultNode) result;


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
								initialResultQuery.addEntity(indexOrgColumn, result);
							}
						} else {
							System.out.println("Add entity: " + i);
							initialResultQuery.addEntity(i, result);
						}
					}

				} else if (result instanceof ResultRelation) {
					System.out.println("Hay relacion tracking mode: " + trackingMode);
					if (!trackingMode) {
						System.out.println("Entro en la relacion");

						it = result.getProperties().entrySet().iterator();
						while (it.hasNext()) {
							Map.Entry entry = (Map.Entry) it.next();
							System.out.println("- " + entry.getKey() + ": " + entry.getValue());
						}
						System.out.println("\n");
						initialResultQuery.addEntity(i, result);
					}
				}
			}
		}

		if (!trackingMode) {
			// Show result table
			TextTable textTable = new TextTable((String[]) initialResultQuery.getColumnsName().toArray(), initialResultQuery.getDataTable());
			textTable.printTable();
			System.out.println("\n\n");
		}
	}

	private int createQueryResult(ResultQuery resultQuery) {
		List<ResultEntity> columnResults;
		String nodeGraphFilesFormat;

		int columnsCount = resultQuery.getColumnsCount();

		if (columnsCount < 1) {
			System.out.println(ErrorConstants.ERR_NODE_CREATION);
			return -1;
		}

		// RETURN n (where n is the node created) is a requirement in the Query format. If the query has been executed correctly
		// there will always a column "n" with node information
		columnResults = resultQuery.getColumn(1);

		for (ResultEntity entity : columnResults) {
			if (entity instanceof ResultNode) {
				return 0;
			}
		}

		return -1;
	}

	private void deleteQueryResult(ResultQuery resultQuery, QueryStructure queryStructure) {

	}
}
