package controllers;

import application.MetadataManager;
import constants.GenericConstants;
import dnl.utils.text.table.TextTable;
import neo4j.*;
import network.MMServer;
import queryStructure.QueryStructure;
import relationsTable.RelationshipsTable;

import java.util.*;

/**
 * Created by Carla Urrea Blázquez on 06/07/2018.
 *
 * QueriesControlles.java
 *
 * For each query, create a controller.
 */
public class QueriesController {
	private MMServer mmServer;
	private QueryExecutor queryExecutor;
	private ResultQuery initialResultQuery;
	private ResultNode rootNode;
	private RelationshipsTable relationshipsTable;


	public QueriesController() {
		mmServer = MMServer.getInstance();
		queryExecutor = new QueryExecutor();
		initialResultQuery = null;
		rootNode = null;
		relationshipsTable = MetadataManager.getInstance().getRelationshipsTable();
	}

	public void  manageNewQuery(QueryStructure queryStructure) {

			// CASE 1: Query's MATCH clause has a relation
			int idRootNode = queryStructure.getRootNodeId();
			System.out.println("ID root node: " + idRootNode);


		if (idRootNode > 0) {
				int partitionID = MetadataManager.getInstance().getMapGraphNodes().get(idRootNode);

				if (partitionID == GenericConstants.MM_SLAVE_NODE_ID) {
					// Root node is inside MetadataManager's Neo4j instance
					queryExecutor.processQuery(queryStructure, this, false);
				} else {
					// Root node is in the slave node with id "partitionID"
					mmServer.sendQuery(partitionID, queryStructure, this, false);
				}

		} else {
			// CASE 2: Query's MATCH clause has not a relation
			mmServer.sendQueryBroadcast(queryStructure, this);
			queryExecutor.processQuery(queryStructure, this, false);
		}
	}

	public void processQueryResults(ResultQuery resultQuery, QueryStructure queryStructure, boolean trackingMode) {
		Iterator it;
		int indexOrgColumn = 0;
		int idBorderNode;


		System.out.println("-> Query Result received");

		if (resultQuery == null) return;

		if (this.initialResultQuery == null) {
			this.initialResultQuery = new ResultQuery(resultQuery.getColumnsName());
		}

		if (!trackingMode) {
			// Send a query to the original root node partition to get its information. This information is needed to replace
			// some conditions in the WHERE clause related with the root node
			// EX:
			//	ORG Query				MOD Query
			// (root.age > m.age)	=>	(14 > m.age)

			System.out.println("Search for root node id: " + queryStructure.getRootNodeId());

			int idPartitionLocal = MetadataManager.getInstance().getMapGraphNodes().get(queryStructure.getRootNodeId());
			String queryRootInfo = "MATCH (n {id:" + queryStructure.getRootNodeId() + " }) RETURN n;";
			ResultQuery resultQueryRootInfo = mmServer.sendStringQuery(idPartitionLocal, queryRootInfo);

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

			if (trackingMode) indexOrgColumn = initialResultQuery.getColumnsName().indexOf(resultQuery.getColumnsName().get(i));

			for (ResultEntity result : columnResults) {

				if (result instanceof ResultNode) {

					System.out.println("Entra en node");

					// TODO: Si es un nodo frontera, hacer el send query pasando la misma instanci y borrar el nodo frontera de la query,
					// activar el modo tracking de sendQuery para concatenar los nuevos resultados y no mostrar aun la tabla al usuario.

					ResultNode resultNode = (ResultNode) result;

					if (resultNode.isBorderNode()) {

						/*
						En el border node actual tengo información del id de la particion a la cual esta sirviendo como embajador.
						Usando el objeto queryStructure podemos recuperar en id del Root node actual y con este id obtener la partición actual
						Con ambas particiones tenemos la key para recuperar el id del border node en la partición forastera
						 */
						int idPartitionLocal = MetadataManager.getInstance().getMapGraphNodes().get(queryStructure.getRootNodeId());
						int idPartitionForeign = resultNode.getForeignPartitionId();
						idBorderNode = resultNode.getNodeId();

						String key = String.valueOf(idPartitionForeign) + String.valueOf(idPartitionLocal);
						System.out.println("Key: " + key );

						int idForeignBorderNode = MetadataManager.getInstance().getMapBoarderNodes().get(key);
						System.out.println("Key: " + key + " - Id node foreign: " + idForeignBorderNode);

						QueryStructure queryStructureModified = queryStructure.replaceRootNode(idForeignBorderNode, rootNode);

						if (idPartitionForeign == 0) {
							queryExecutor.processQuery(queryStructure, this, true);
						} else {
							mmServer.sendQuery(idPartitionForeign, queryStructureModified, this, true);
						}

						System.out.println("Salgo de border. Tracking: " + trackingMode);
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
						while(it.hasNext()) {
							Map.Entry entry = (Map.Entry)it.next();
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
			TextTable textTable = new TextTable((String[])initialResultQuery.getColumnsName().toArray(), initialResultQuery.getDataTable());
			textTable.printTable();
			System.out.println("\n\n");
		}
	}
}
