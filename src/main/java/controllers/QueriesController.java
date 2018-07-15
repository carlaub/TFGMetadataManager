package controllers;

import application.MetadataManager;
import constants.GenericConstants;
import dnl.utils.text.table.TextTable;
import neo4j.*;
import network.MMServer;
import queryStructure.QueryStructure;

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
	private QueryStructure currentQueryStructure;
	private int columnsCount;
	private ResultNode rootNode;


	public QueriesController() {
		mmServer = MMServer.getInstance();
		queryExecutor = new QueryExecutor();
		initialResultQuery = null;
		rootNode = null;
	}

	public void  manageNewQuery(QueryStructure queryStructure) {

		this.currentQueryStructure = queryStructure;
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

		columnsCount = resultQuery.getColumnsCount();
		for (int i = 0; i < columnsCount; i++) {
			List<ResultEntity> columnResults = resultQuery.getColumn(i);

			for (ResultEntity result : columnResults) {

				if (result instanceof ResultNode) {

					// TODO: Si es un nodo frontera, hacer el send query pasando la misma instanci y borrar el nodo frontera de la query,
					// activar el modo tracking de sendQuery para concatenar los nuevos resultados y no mostrar aun la tabla al usuario.

					ResultNode resultNode = (ResultNode) result;

					if (resultNode.isBorderNode()) {

						/*
						En el border node actual tengo información del id de la particion a la cual esta sirviendo como embajador.
						Usando el objeto queryStructure podemos recuperar en id del Root node actual y con este id obtener la partición actual
						Con ambas particiones tenemos la key para recuperar el id del border node en la partición forastera
						 */
						// TODO: Reformat query

						int idPartitionLocal = MetadataManager.getInstance().getMapGraphNodes().get(queryStructure.getRootNodeId());
						int idPartitionForeign = resultNode.getForeignPartitionId();

						String key = String.valueOf(idPartitionLocal) + String.valueOf(idPartitionForeign);
						System.out.println("Key: " + key );

						int idForeignBorderNode = MetadataManager.getInstance().getMapBoarderNodes().get(key);
						System.out.println("Key: " + key + " - Id node foreign: " + idForeignBorderNode);

						QueryStructure queryStructureModified = queryStructure.replaceRootNode(idForeignBorderNode, rootNode);

						mmServer.sendQuery(idPartitionForeign, queryStructureModified, this, true);

					} else if (!trackingMode) {
						initialResultQuery.addEntity(i, resultNode);
					}

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
			System.out.println("\n\n");
		}
	}
}
