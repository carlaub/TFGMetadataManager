package neo4j;

import application.MetadataManager;
import constants.ErrorConstants;
import constants.GenericConstants;
import controllers.QueriesController;
import network.MMServer;
import org.neo4j.graphdb.*;
import queryStructure.QueryStructure;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Carla Urrea Blázquez on 06/06/2018.
 *
 * QueryExecutor.java
 */
public class QueryExecutor {
	private GraphDatabaseService graphDatabaseService;

	public QueryExecutor() {
		graphDatabaseService = GraphDatabase.getInstance().getDataBaseGraphService();
	}

	public List<ResultEntity> processQuery(String query, QueriesController queriesController) {
		try (Transaction q = graphDatabaseService.beginTx();
			 Result result = graphDatabaseService.execute(query)) {

			List<ResultEntity> list = new ArrayList<>();

			System.out.println("HAS NEXT: " + result.hasNext());

			while (result.hasNext()) {

				Node node = (Node)result.next().get("n");
				if (node != null) {
					ResultNode resultNode = new ResultNode();

					Iterable<String> properties = node.getPropertyKeys();
					Iterable<Label> labels = node.getLabels();

					for (String propertyKey : properties) resultNode.addProperty(propertyKey, node.getProperty(propertyKey));

					for (Label label : labels) resultNode.addLabel(label.name());

					list.add(resultNode);
				} else {
					// Is Relation
					Relationship relationship = (Relationship) result.next().get("r");

					if (relationship != null) {
						ResultRelation resultRelation = new ResultRelation();

						Iterable<String> properties = relationship.getPropertyKeys();

						for (String propertyKey : properties) {
							resultRelation.addProperty(propertyKey, relationship.getProperty(propertyKey));
						}

						list.add(resultRelation);
					}
				}
			}

			queriesController.processQueryResults(list);

			// Important to avoid unwanted behaviour, such as leaking transactions
			result.close();

			return list;
		}
	}
}
