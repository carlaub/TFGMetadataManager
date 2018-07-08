package controllers;

import application.MetadataManager;
import constants.ErrorConstants;
import constants.GenericConstants;
import neo4j.QueryExecutor;
import neo4j.ResultEntity;
import neo4j.ResultNode;
import neo4j.ResultRelation;
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

		if (queryStructure.hasRelation()) {
			// CASE 1: Query's MATCH clause has a relation

			int idRootNode = queryStructure.getRootNodeId();
			System.out.println("ID root node: " + idRootNode);
			if (idRootNode < 0) {
				int partitionID = MetadataManager.getInstance().getMapGraphNodes().get(idRootNode);
				String queryString = queryStructure.toString();

				if (partitionID == GenericConstants.MM_SLAVE_NODE_ID) {
					// Root node is inside MetadataManager's Neo4j instance
					queryExecutor.processQuery(queryString, this);
				} else {
					// Root node is in the slave node with id "partitionID"
					mmServer.sendQuery(partitionID, queryString, this);
				}
			} else {
				System.out.println(ErrorConstants.ERR_QUERY_ROOT_NODE_ID);
			}
		} else {
			// CASE 2: Query's MATCH clause has not a relation
			mmServer.sendQueryBroadcast(queryStructure, this);
			queryExecutor.processQuery(queryStructure.toString(), this);
		}
	}

	public void processQueryResults(List<ResultEntity> results) {
		Iterator it;

		// TODO: Filtrar nodos frontera
		System.out.println("-> List Result received");
		if (results != null) System.out.println("Size: " + results.size());

		for (ResultEntity result : results) {

			if (result instanceof ResultNode) {
				ResultNode resultNode = (ResultNode) result;

				// If the node is border, create the modifies query and send it to the correct partition
//				if (resultNode.isBorderNode()) {
//
//				} else {

					List<String> labels = resultNode.getLabels();

					System.out.println("Node ");

					// Labels
					if (!labels.isEmpty()) {
						int listSize = labels.size();

						System.out.print("{ ");
						for (int i = 0; i < listSize - 1; i++) {
							System.out.print(labels.get(i) + ": ");
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
//				}

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


	private void followBorderNode() {

	}
}
