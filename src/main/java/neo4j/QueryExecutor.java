package neo4j;

import controllers.QueriesController;
import org.neo4j.graphdb.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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

	public ResultQuery processQuery(String query, QueriesController queriesController, boolean trackingMode) {
		try (Transaction q = graphDatabaseService.beginTx();
			 Result result = graphDatabaseService.execute(query)) {

			ResultQuery resultQuery = new ResultQuery();
			List<String> columnNames = result.columns();
			int columnsCount;

			resultQuery.setColumnsName(columnNames);
			columnsCount = columnNames.size();

			for (int i = 0; i < columnsCount; i++) {
				ResourceIterator columnIterator = result.columnAs(columnNames.get(i));

				while (columnIterator.hasNext()) {
					Map<String, Object> next = (Map<String, Object>) columnIterator.next();

					Node node = (Node) next.get("n");
					if (node != null) {
						ResultNode resultNode = new ResultNode();

						Iterable<String> properties = node.getPropertyKeys();
						Iterable<Label> labels = node.getLabels();

						for (String propertyKey : properties)
							resultNode.addProperty(propertyKey, node.getProperty(propertyKey));

						for (Label label : labels) resultNode.addLabel(label.name());

						resultQuery.addEntity(i, resultNode);
					} else {
						// Is Relation
						System.out.println("Is relation??");
						Relationship relationship = (Relationship) next.get("r");
						if (relationship != null) {
							ResultRelation resultRelation = new ResultRelation();

							Iterable<String> properties = relationship.getPropertyKeys();

							for (String propertyKey : properties) {
								resultRelation.addProperty(propertyKey, relationship.getProperty(propertyKey));
							}

							resultRelation.setStartNodeId(relationship.getStartNodeId());
							resultRelation.setEndNodeId(relationship.getEndNodeId());

							resultQuery.addEntity(i, resultRelation);
						}
					}
				}
			}

			queriesController.processQueryResults(resultQuery, trackingMode);

			// Important to avoid unwanted behaviour, such as leaking transactions
			result.close();

			return resultQuery;
		}
	}
}
