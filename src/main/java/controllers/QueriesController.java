package controllers;

import application.MetadataManager;
import constants.GenericConstants;
import dnl.utils.text.table.TextTable;
import neo4j.*;
import network.MMServer;
import queryStructure.QueryStructure;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

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
	private QueryStructure currentQuery;


	public QueriesController() {
		mmServer = MMServer.getInstance();
		queryExecutor = new QueryExecutor();
	}

	public void  manageNewQuery(QueryStructure queryStructure) {
		currentQuery = queryStructure;

//		if (queryStructure.hasRelation()) {
			// CASE 1: Query's MATCH clause has a relation

			int idRootNode = queryStructure.getRootNodeId();
			System.out.println("ID root node: " + idRootNode);

			//TODO: Descomentar y eliminar
			String queryString = queryStructure.toString();
			queryExecutor.processQuery(queryString, this, false);

//		if (idRootNode > 0) {
//				int partitionID = MetadataManager.getInstance().getMapGraphNodes().get(idRootNode);
//				String queryString = queryStructure.toString();
//
//				if (partitionID == GenericConstants.MM_SLAVE_NODE_ID) {
//					// Root node is inside MetadataManager's Neo4j instance
//					queryExecutor.processQuery(queryString, this, false);
//				} else {
//					// Root node is in the slave node with id "partitionID"
//					mmServer.sendQuery(partitionID, queryString, this, false);
//				}
////			} else {
////				System.out.println(ErrorConstants.ERR_QUERY_ROOT_NODE_ID);
////			}
//		} else {
//			// CASE 2: Query's MATCH clause has not a relation
//			mmServer.sendQueryBroadcast(queryStructure, this);
//			queryExecutor.processQuery(queryStructure.toString(), this, false);
//		}
	}

	public void processQueryResults(ResultQuery resultQuery, boolean trackingMode) {
		Iterator it;

		// TODO: Filtrar nodos frontera
		System.out.println("-> Query Result received");

		if (resultQuery == null) return;

		int columnsCount = resultQuery.getColumnsCount();

		for (int i = 0; i < columnsCount; i++) {
			List<ResultEntity> columnResults = resultQuery.getColumn(i);

			for (ResultEntity result : columnResults) {

				if (result instanceof ResultNode) {

					// TODO: Si es un nodo frontera, hacer el send query pasando la misma instanci y borrar el nodo frontera de la query,
					// activar el modo tracking de sendQuery para concatenar los nuevos resultados y no mostrar aun la tabla al usuario.

					ResultNode resultNode = (ResultNode) result;

					List<String> labels = resultNode.getLabels();

					// Labels
					if (!labels.isEmpty()) {
						int listSize = labels.size();

						System.out.print("{ ");
						for (int j = 0; j < listSize - 1; j++) {
							System.out.print(labels.get(j) + ": ");
						}
						System.out.print(labels.get(listSize - 1) + " }");
					}

					// Properties
					it = result.getProperties().entrySet().iterator();
					while(it.hasNext()) {
						Map.Entry entry = (Map.Entry)it.next();
						System.out.print(", " + entry.getKey() + ": " + entry.getValue());
					}
					System.out.println("\n");

				} else if (result instanceof ResultRelation) {
					it = result.getProperties().entrySet().iterator();
					while(it.hasNext()) {
						Map.Entry entry = (Map.Entry)it.next();
						System.out.println("- " + entry.getKey() + ": " + entry.getValue());
					}
					System.out.println("\n");
				}
			}
		}

		if (!trackingMode) {
			// Show result table
			TextTable textTable = new TextTable(resultQuery.getColumnsName(), resultQuery.getDataTable());
			textTable.printTable();
		}
	}
}
